import json
import time
from collections import defaultdict
from typing import List, Dict, Any, Optional
from confluent_kafka import Consumer, KafkaException, TopicPartition
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_PREFIX,
    BATCH_SIZE,
    BUFFER_SIZE,
    POLL_TIMEOUT,
    MAX_POLL_RECORDS,
)


class CameraDataConsumer:
    def __init__(self, camera_id: str, group_id: str = None):
        self.camera_id = camera_id
        self.topic = f"{KAFKA_TOPIC_PREFIX}_{camera_id}"
        self.group_id = group_id or f"consumer_group_{camera_id}"
        
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': self.group_id,
            'auto.offset.reset': 'earliest',  # 从头开始消费
            'enable.auto.commit': False,      # 手动提交 offset
            'max.poll.records': MAX_POLL_RECORDS,
        })
        self.consumer.subscribe([self.topic])
    
    def _fetch_batch(self) -> List[Dict[str, Any]]:
        """
        步骤1：取大约750条数据，保留最后时间点的数据不取
        无数据则等待固定时间
        """
        messages = []
        start_time = time.time()
        
        while len(messages) < BATCH_SIZE + BUFFER_SIZE:
            # 计算剩余等待时间
            elapsed = time.time() - start_time
            timeout = max(0, POLL_TIMEOUT - elapsed)
            
            msg = self.consumer.poll(timeout=timeout)
            
            if msg is None:
                # 超时，没有更多数据
                break
            elif msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # 到达分区末尾
                    break
                else:
                    raise KafkaException(msg.error())
            else:
                value = json.loads(msg.value().decode('utf-8'))
                messages.append({
                    'offset': msg.offset(),
                    'data': value,
                })
        
        if not messages:
            return []
        
        # 找出最后的时间点
        last_timestamp = max(m['data']['timestamp'] for m in messages)
        
        # 过滤掉最后时间点的数据（不取出来）
        filtered = [m for m in messages if m['data']['timestamp'] < last_timestamp]
        
        # 确保至少有 BUFFER_SIZE 条数据用于后续判断
        if len(filtered) > BUFFER_SIZE:
            filtered = filtered[:BATCH_SIZE]  # 截取到 BATCH_SIZE
        
        return filtered
    
    def _process_batch(self, messages: List[Dict[str, Any]]) -> List[int]:
        """
        步骤2：遍历数据，按规则提取并返回需要删除的 offset 列表
        
        规则：从头遍历，若第一条数据的 id 为 A，
        且最后一条 id 为 A 的数据没有出现在末尾 60 条中，
        则所有 id 为 A 的数据全部提取处理并从 Redis 删除。
        重复直到遍历完。
        """
        if len(messages) <= BUFFER_SIZE:
            # 数据量不足，无法判断，全部保留
            return []
        
        offsets_to_delete = []
        remaining = messages[:-BUFFER_SIZE]  # 可处理区域（排除末尾缓冲区）
        buffer_zone = messages[-BUFFER_SIZE:]  # 末尾缓冲区
        buffer_ids = {m['data']['id'] for m in buffer_zone}
        
        i = 0
        while i < len(remaining):
            current_id = remaining[i]['data']['id']
            
            # 找到该 id 在 remaining 中的最后一条
            last_idx = i
            for j in range(i, len(remaining)):
                if remaining[j]['data']['id'] == current_id:
                    last_idx = j
            
            # 检查该 id 的最后一条是否出现在末尾缓冲区中
            if current_id not in buffer_ids:
                # 符合条件：提取所有该 id 的数据并标记删除
                for j in range(i, last_idx + 1):
                    offsets_to_delete.append(remaining[j]['offset'])
                    # 在这里调用你的业务处理逻辑
                    self._handle_data(remaining[j]['data'])
                
                # 跳过已处理的数据
                i = last_idx + 1
            else:
                # 不符合条件，跳过这一条，继续检查下一条
                i += 1
        
        return offsets_to_delete
    
    def _handle_data(self, data: Dict[str, Any]):
        """业务处理逻辑（用户自定义）"""
        # TODO: 在这里实现你的业务处理
        print(f"Processing: camera={data['camera']}, id={data['id']}, timestamp={data['timestamp']}")
    
    def _commit_offsets(self, offsets: List[int]):
        """提交已处理的 offset"""
        if not offsets:
            return
        
        # 对 offset 排序，提交最大的那个（Kafka 提交的是下一条要消费的 offset）
        max_offset = max(offsets)
        topic_partition = TopicPartition(self.topic, 0, max_offset + 1)
        self.consumer.commit(offsets=[topic_partition], asynchronous=False)
    
    def run(self):
        """主循环：重复步骤1到2"""
        print(f"Consumer started for camera: {self.camera_id}")
        
        try:
            while True:
                # 步骤1：取数据
                messages = self._fetch_batch()
                
                if not messages:
                    # 无数据，等待后继续
                    time.sleep(POLL_TIMEOUT)
                    continue
                
                # 步骤2：处理数据，获取需要删除的 offset
                offsets_to_delete = self._process_batch(messages)
                
                # 提交 offset（相当于从 Kafka 中"删除"已处理的数据）
                if offsets_to_delete:
                    self._commit_offsets(offsets_to_delete)
                
        except KeyboardInterrupt:
            print("\nConsumer stopped.")
        finally:
            self.consumer.close()


# ============ 使用示例 ============
if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python consumer.py <camera_id> [group_id]")
        sys.exit(1)
    
    camera_id = sys.argv[1]
    group_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    consumer = CameraDataConsumer(camera_id, group_id)
    consumer.run()