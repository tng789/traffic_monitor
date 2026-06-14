import json
from typing import Dict, Any

from collections import deque

def parse_kafka_message(msg) -> Dict[str, Any]:
    """解析 Kafka 消息"""
    if msg is None or msg.error():
        return None
    return json.loads(msg.value().decode('utf-8'))


def create_topic_if_not_exists(admin_client, topic_name: str, num_partitions: int = 1):
    """创建 Topic（如果不存在）"""
    from confluent_kafka.admin import NewTopic
    
    topics = admin_client.list_topics().topics
    if topic_name not in topics:
        new_topic = NewTopic(topic_name, num_partitions=num_partitions)
        admin_client.create_topics([new_topic])
        print(f"Created topic: {topic_name}")
