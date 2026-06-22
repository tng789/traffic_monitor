import pika
import json
import threading
import time
import sys
import argparse
import psutil
import os
from track_processing import track_processor
import subprocess

from app_log import setup_logging, get_logger

logger = get_logger(__name__)


def get_process_by_camera_id(camera_id):
    """
    查找处理指定camera_id的进程
    """
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            # 检查进程命令行是否包含我们的程序和指定的camera_id
            cmdline = proc.info['cmdline']
            if 'python' in cmdline[0]:
                if len(cmdline) >= 4 and 'rmq.py' in cmdline[1] and 'start' in cmdline[2]  and cmdline[3] == camera_id:
                    # 排除当前进程，只查找其他进程
                    if proc.pid != current_pid:
                        return proc
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
        except (IndexError, TypeError) as e:
            logger.debug("跳过无法解析的进程信息: %s", e)
            continue
    return None


def start_process(camera_id):
    """
    启动处理指定camera_id的进程
    """
    # 检查是否已经有处理该camera_id的进程在运行
    existing_process = get_process_by_camera_id(camera_id)
    if existing_process:
        logger.warning(
            "已存在处理 camera_id=%s 的进程 (PID=%s)，拒绝重复启动",
            camera_id, existing_process.pid,
        )
        return False
    
    # 启动新的消费者进程
    amqp_url = 'amqp://admin:zhxk12345@192.168.1.142:5672/'
    # amqp_url = 'amqp://admin:admin123@192.168.31.82:5672/'

    processor = track_processor(camera_id=camera_id)

    consumer = Consumer(
        amqp_url = amqp_url,
        queue_name = camera_id,
        routing_key_pattern = camera_id,
        consumer_name = f'consumer_{camera_id}',
        message_ttl_ms=86400000,   # 24小时
        max_queue_length=10000,
        max_retries=3
    )
    
    thread_a = threading.Thread(target=consumer.consume, args=(processor.process_message,), daemon=True)
    thread_a.start()

    logger.info("已启动 camera_id=%s 的消费者线程 (PID=%s)", camera_id, os.getpid())

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("收到退出信号，停止 camera_id=%s 的消费者", camera_id)
        return True


def start_daemon_process(camera_id):
    """
    以守护进程方式启动处理指定camera_id的进程
    """
    # 检查是否已经有处理该camera_id的进程在运行
    existing_process = get_process_by_camera_id(camera_id)
    if existing_process:
        logger.warning(
            "已存在处理 camera_id=%s 的进程 (PID=%s)，拒绝重复启动",
            camera_id, existing_process.pid,
        )
        return False

    # 使用subprocess在后台启动新进程
    cmd = [sys.executable, __file__, 'start', camera_id]
    # 启动进程并在后台运行
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,  # 重定向输出到空设备
        stderr=subprocess.DEVNULL,  # 重定向错误输出到空设备
        stdin=subprocess.DEVNULL,   # 重定向输入到空设备
        preexec_fn=os.setsid if os.name != 'nt' else None  # 在Unix系统上创建新会话组
    )
    
    logger.info("已在后台启动 camera_id=%s 的消费者 (PID=%s)", camera_id, process.pid)
    return True


def stop_process(camera_id):
    """
    停止处理指定camera_id的进程
    """
    existing_process = get_process_by_camera_id(camera_id)
    if existing_process:
        try:
            logger.info(
                "终止 camera_id=%s 的消费者进程 PID=%s",
                camera_id, existing_process.pid,
            )
            existing_process.terminate()
            existing_process.wait(timeout=5)
        except psutil.TimeoutExpired:
            logger.warning("进程 PID=%s 未响应 terminate，强制 kill", existing_process.pid)
            existing_process.kill()
        except psutil.NoSuchProcess:
            logger.warning("进程 PID=%s 已不存在", existing_process.pid)
        logger.info("camera_id=%s 的消费者进程已终止", camera_id)
        return True
    else:
        logger.warning("未找到 camera_id=%s 对应的消费者进程", camera_id)
        return False


def clear_queue_for_camera(camera_id):
    """
    清理指定camera_id的队列中的数据
    """
    amqp_url = 'amqp://admin:zhxk12345@192.168.1.142:5672/'
    
    try:
        # 创建临时连接来清空队列
        params = pika.URLParameters(amqp_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        
        # 声明队列（如果不存在则创建，如果存在则获取信息）
        queue_name = camera_id
        channel.queue_declare(queue=queue_name, durable=True)
        
        # 获取队列消息数量
        method = channel.queue_declare(queue=queue_name, passive=True)
        message_count = method.method.message_count
        
        if message_count > 0:
            logger.info("发现 %s 条消息在队列 %s 中，开始清空...", message_count, queue_name)
            
            # 一次性清空队列中的所有消息
            purged_count = channel.queue_purge(queue=queue_name)
            logger.info("已清空 %s 条消息从队列 %s", purged_count, queue_name)
        else:
            logger.info("队列 %s 中没有消息需要清空", queue_name)
        
        connection.close()
        return True
    except Exception as e:
        logger.error("清空队列 %s 时发生错误: %s", camera_id, e)
        return False


# ===== 生产者（带连接重试）=====
class Producer:
    def __init__(self, amqp_url, max_retries=5):
        self.amqp_url = amqp_url
        self.max_retries = max_retries
        self.connection = self._create_connection()
        self.channel = self.connection.channel()
        self._setup_exchange()

    def _create_connection(self):
        """创建连接，带重试机制"""
        for attempt in range(self.max_retries):
            try:
                params = pika.URLParameters(self.amqp_url)
                params.heartbeat = 30
                params.connection_attempts = 3
                params.retry_delay = 2
                connection = pika.BlockingConnection(params)
                logger.info("RabbitMQ 连接成功")
                return connection
            except Exception as e:
                logger.warning("RabbitMQ 连接失败 (第%s次): %s", attempt + 1, e)
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # 指数退避

    def _setup_exchange(self):
        """声明主交换机和死信交换机"""
        # 主交换机
        self.channel.exchange_declare(
            exchange='data_router',
            exchange_type='topic',
            durable=True
        )
        # 死信交换机（用于接收处理失败的消息）
        self.channel.exchange_declare(
            exchange='dlx_exchange',
            exchange_type='direct',
            durable=True
        )

    def publish(self, routing_key, data, ttl_ms=None):
        """
        发送消息
        :param routing_key: 路由键，如 'A', 'B'
        :param data: 消息体（字典）
        :param ttl_ms: 消息TTL（毫秒），None表示永不过期
        """
        message = json.dumps(data)
        
        # 设置消息属性
        properties = pika.BasicProperties(
            delivery_mode=2,  # 持久化
            expiration=str(ttl_ms) if ttl_ms else None  # 消息TTL
        )
        
        self.channel.basic_publish(
            exchange='data_router',
            routing_key=routing_key,
            body=message.encode('utf-8'),
            properties=properties
        )
        logger.debug("[生产者] 发送 routing_key=%s ttl=%sms", routing_key, ttl_ms)

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()


# ===== 消费者（带死信队列、TTL、限流）=====
class Consumer:
    def __init__(
        self,
        amqp_url,
        queue_name,
        routing_key_pattern,
        consumer_name,
        # 队列配置
        message_ttl_ms=86400000,      # 消息24小时过期
        max_queue_length=10000,       # 队列最多10000条
        max_retries=3,                # 最大重试次数
        prefetch_count=1              # 每次只取1条，公平分发
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.routing_key_pattern = routing_key_pattern
        self.consumer_name = consumer_name
        self.max_retries = max_retries
        self.prefetch_count = prefetch_count
        
        self.connection = self._create_connection()
        self.channel = self.connection.channel()
        
        self._setup_exchanges()
        self._setup_queues(message_ttl_ms, max_queue_length)

    def _create_connection(self):
        """创建连接，带重试机制"""
        params = pika.URLParameters(self.amqp_url)
        params.heartbeat = 30
        params.connection_attempts = 3
        params.retry_delay = 2
        try:
            connection = pika.BlockingConnection(params)
            logger.info("[%s] RabbitMQ 连接成功", self.consumer_name)
            return connection
        except Exception:
            logger.exception("[%s] RabbitMQ 连接失败", self.consumer_name)
            raise

    def _setup_exchanges(self):
        """声明主交换机和死信交换机"""
        # 主交换机
        self.channel.exchange_declare(
            exchange='data_router',
            exchange_type='topic',
            durable=True
        )
        # 死信交换机
        self.channel.exchange_declare(
            exchange='dlx_exchange',
            exchange_type='direct',
            durable=True
        )

    def _setup_queues(self, message_ttl_ms, max_queue_length):
        """
        声明主队列和死信队列
        """
        # 1. 声明死信队列（接收处理失败的消息）
        dlx_queue_name = f"dlx_{self.queue_name}"
        self.channel.queue_declare(
            queue=dlx_queue_name,
            durable=True,
            arguments={
                'x-message-ttl': 604800000,  # 死信队列中的消息7天后过期
                'x-max-length': 1000         # 死信队列最多1000条
            }
        )
        # 死信队列绑定到死信交换机
        self.channel.queue_bind(
            exchange='dlx_exchange',
            queue=dlx_queue_name,
            routing_key=f"dlx_{self.queue_name}"
        )

        # 2. 声明主队列，配置死信转发、TTL、长度限制
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={
                # 消息过期或拒绝后，转发到死信交换机
                'x-dead-letter-exchange': 'dlx_exchange',
                'x-dead-letter-routing-key': f"dlx_{self.queue_name}",
                # 消息TTL（毫秒）
                'x-message-ttl': message_ttl_ms,
                # 队列最大长度
                'x-max-length': max_queue_length,
                # 超出长度时的策略：reject-publish（拒绝新消息）或 drop-head（丢弃最老消息）
                'x-overflow': 'reject-publish'
            }
        )

        # 主队列绑定到主交换机
        self.channel.queue_bind(
            exchange='data_router',
            queue=self.queue_name,
            routing_key=self.routing_key_pattern
        )

        logger.info(
            "[%s] 队列 '%s' 初始化完成 (DLQ='%s', TTL=%sms, max_length=%s)",
            self.consumer_name, self.queue_name, dlx_queue_name,
            message_ttl_ms, max_queue_length,
        )

    def consume(self, callback):
        """开始消费"""
        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._wrap_callback(callback),
            auto_ack=False
        )
        logger.info("[%s] 开始消费，等待消息...", self.consumer_name)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("[%s] 收到退出信号，正在关闭...", self.consumer_name)
            self.channel.stop_consuming()

    def _wrap_callback(self, callback):
        def wrapper(ch, method, properties, body):
            retry_count = self._get_retry_count(properties)
            try:
                data = json.loads(body.decode('utf-8'))
                logger.debug(
                    "[%s] 收到消息 track_id=%s frame_id=%s (重试第%s次)",
                    self.consumer_name,
                    data.get('track_id'),
                    data.get('frame_id'),
                    retry_count,
                )
                callback(data)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                logger.exception("[%s] 消息处理失败 (重试第%s次)", self.consumer_name, retry_count)
                if retry_count < self.max_retries:
                    logger.warning("[%s] 准备第%s次重试", self.consumer_name, retry_count + 1)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                else:
                    logger.error("[%s] 超过最大重试次数，转入死信队列", self.consumer_name)
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        return wrapper

    def _get_retry_count(self, properties):
        """从消息头中获取重试次数"""
        if properties.headers and 'x-retry-count' in properties.headers:
            return properties.headers['x-retry-count']
        return 0


# ===== 实际创建对象并运行 =====
if __name__ == '__main__':
    setup_logging()
    if len(sys.argv) < 3:
        logger.error("用法: python rmq.py start|start-daemon|stop <camera_id>")
        sys.exit(1)

    command = sys.argv[1]
    camera_id = sys.argv[2]
    logger.info("执行命令 command=%s camera_id=%s", command, camera_id)

    if command == 'start':
        start_process(camera_id)
    elif command == 'start-daemon':
        start_daemon_process(camera_id)
    elif command == 'stop':
        stop_process(camera_id)
    else:
        logger.error("无效命令: %s，请使用 start / start-daemon / stop", command)
        sys.exit(1)