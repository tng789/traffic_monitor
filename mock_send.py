'''
从文件中读出，模拟发送数据给rabbitMQ
'''
import json
import time
from rmq import Producer


def send_json_from_file_to_rabbitmq(file_path, camera_id, amqp_url='amqp://admin:admin123@192.168.31.82:5672/'):
    """
    从文件读取JSON对象并发送到RabbitMQ
    Args:
        file_path: JSON文件路径
        camera_id: 相机ID，用作routing key
        amqp_url: RabbitMQ连接地址
    """
    # 创建Producer实例
    producer = Producer(amqp_url)
    
    try:
        # 读取文件内容
        with open(file_path, 'r') as file:
            content = file.read().strip()
            
        # 按'}{'模式分割JSON对象（连续JSON对象的常见格式）
        json_strings = []
        start_idx = 0
        
        # 查找所有'}{'分隔符位置
        i = 0
        while i < len(content) - 1:
            if content[i:i+2] == '}{':
                # 找到一个JSON对象的结束和下一个的开始
                json_strings.append(content[start_idx:i+1])
                start_idx = i + 1  # 下一个JSON对象从'{'开始
            i += 1
            
        # 添加最后一个JSON对象
        if start_idx < len(content):
            json_strings.append(content[start_idx:])
        
        # 处理并发送每个JSON对象
        count = 0
        for idx, json_str in enumerate(json_strings):
            try:
                # 解析JSON
                detection_data = json.loads(json_str.strip())
                
                # 发送数据到RabbitMQ
                producer.publish(camera_id, detection_data, ttl_ms=86400000)
                
                print(f"已发送数据: frame_id={detection_data.get('frame_id')}, track_id={detection_data.get('track_id')}")
                count += 1
                
                # 短暂延时，避免发送过快
                time.sleep(0.01)
                
            except json.JSONDecodeError as e:
                print(f"JSON解析错误 (对象 {idx+1}): {e}")
                print(f"问题内容: {json_str[:100]}...")
            except Exception as e:
                print(f"发送消息时出错 (对象 {idx+1}): {e}")
                
        print(f"总共发送了 {count} 条数据")
        
    except FileNotFoundError:
        print(f"文件 {file_path} 未找到")
    except Exception as e:
        print(f"读取文件时出错 {file_path}: {e}")
    finally:
        # 关闭连接
        producer.close()


if __name__ == "__main__":
    # 定义文件路径和相机ID
    file_path = "time_redlight_330282000000010162.txt"
    camera_id = "330282000000010162"
    send_json_from_file_to_rabbitmq(file_path, camera_id)