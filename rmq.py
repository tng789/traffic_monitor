import pika
import json
import threading
import time
import logging

from track_processing import track_processor
import sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



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
                logging.info("RabbitMQ 连接成功")
                return connection
            except Exception as e:
                logging.warning(f"连接失败 (第{attempt + 1}次): {e}")
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
        logging.info(f"[生产者] 发送: routing_key={routing_key}, data={data}, ttl={ttl_ms}ms")

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
        return pika.BlockingConnection(params)

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

        logging.info(f"[{self.consumer_name}] 队列 '{self.queue_name}' 初始化完成")
        logging.info(f"    死信队列: '{dlx_queue_name}', TTL: {message_ttl_ms}ms, 最大长度: {max_queue_length}")

    def consume(self, callback):
        """开始消费"""
        self.channel.basic_qos(prefetch_count=self.prefetch_count)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self._wrap_callback(callback),
            auto_ack=False
        )
        logging.info(f"[{self.consumer_name}] 开始消费，等待消息...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logging.info(f"[{self.consumer_name}] 收到退出信号，正在关闭...")
            self.channel.stop_consuming()

    def _wrap_callback(self, callback):
        def wrapper(ch, method, properties, body):
            retry_count = self._get_retry_count(properties)
            try:
                data = json.loads(body.decode('utf-8'))
                logging.info(f"[{self.consumer_name}] 收到 (重试第{retry_count}次): {data}")
                callback(data)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logging.info(f"[{self.consumer_name}] 处理成功，已ACK")
            except Exception as e:
                logging.error(f"[{self.consumer_name}] 处理失败: {e}")
                if retry_count < self.max_retries:
                    # 重新入队，稍后重试
                    logging.warning(f"[{self.consumer_name}] 第{retry_count + 1}次重试...")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
                else:
                    # 超过最大重试次数，拒绝且不重新入队 → 进入死信队列
                    logging.error(f"[{self.consumer_name}] 超过最大重试次数，转入死信队列")
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        return wrapper

    def _get_retry_count(self, properties):
        """从消息头中获取重试次数"""
        if properties.headers and 'x-retry-count' in properties.headers:
            return properties.headers['x-retry-count']
        return 0


# ===== 实际创建对象并运行 =====
if __name__ == '__main__':
    amqp_url = 'amqp://admin:zhxk12345@192.168.1.142:5672/'

    processor = track_processor(camera_id=sys.argv[1])
#    # --- 启动消费者A（只收 key 为 A 的数据）---
#    def track_processing(data):
#        print(f"  >>> 消费者A处理业务逻辑: {data}")
#        # 模拟偶尔处理失败
#        if data.get('id') == 3:
#            raise Exception("模拟处理失败")
#        time.sleep(1)

    camera_id = sys.argv[1]
    consumer = Consumer(
        amqp_url = amqp_url,
        queue_name = camera_id,
        routing_key_pattern = camera_id,
        consumer_name = f'consumer_{camera_id}',
        message_ttl_ms=86400000,   # 24小时
        max_queue_length=10000,
        max_retries=3
    )
    # thread_a = threading.Thread(target=consumer.consume, args=(track_processing,), daemon=True)

    thread_a = threading.Thread(target=consumer.consume, args=(processor.process_message,), daemon=True)
    thread_a.start()

#
#    # --- 启动消费者B（只收 key 为 B 的数据）---
#    def callback_b(data):
#        print(f"  >>> 消费者B处理业务逻辑: {data}")
#        time.sleep(1)
#
#    consumer_b = Consumer(
#        amqp_url=amqp_url,
#        queue_name='queue_B',
#        routing_key_pattern='B',
#        consumer_name='消费者B',
#        message_ttl_ms=86400000,
#        max_queue_length=10000,
#        max_retries=3
#    )
#    thread_b = threading.Thread(target=consumer_b.consume, args=(callback_b,), daemon=True)
#    thread_b.start()

 #   # --- 启动生产者，发送测试数据 ---
 #   producer = Producer(amqp_url)
 #   time.sleep(1)

 #   test_data = [
 #       ('A', {'id': 1, 'value': '这是A的数据'}),
 #       ('B', {'id': 2, 'value': '这是B的数据'}),
 #       ('A', {'id': 3, 'value': '这条会失败并进入死信队列'}),
 #       ('B', {'id': 4, 'value': '这是B的数据'}),
 #   ]

 #   for key, data in test_data:
 #       producer.publish(key, data, ttl_ms=86400000)  # 每条消息24小时TTL
 #       time.sleep(0.5)

 #   producer.close()

    # 保持主线程运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n程序退出")