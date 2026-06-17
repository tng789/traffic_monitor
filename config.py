# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_PREFIX = 'camera'  # Topic 命名前缀

# 消费参数
BATCH_SIZE = 750          # 每次最多取多少条
BUFFER_SIZE = 60          # 末尾缓冲区大小
POLL_TIMEOUT = 1.0        # 无数据时等待时间（秒）
MAX_POLL_RECORDS = 1000   # 单次 poll 最大记录数（略大于 BATCH_SIZE）

# 生产者配置
PRODUCER_ACKS = 'all'     # 确保消息不丢失