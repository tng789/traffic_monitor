from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import sys
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PREFIX

def consume_messages(camera_ids=None, group_id='test_group'):
    """
    Consume messages from Kafka topics and print them
    
    Args:
        camera_ids: List of camera IDs to subscribe to. If None, subscribes to all topics with the prefix
        group_id: Consumer group ID
    """
    # Consumer configuration
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'latest',  # Start from latest messages
        'enable.auto.commit': True,     # Auto-commit offsets
        'auto.commit.interval.ms': 1000 # Commit every 1 second
    }
    
    # Create consumer instance
    consumer = Consumer(consumer_config)
    
    try:
        # Determine topics to subscribe to
        if camera_ids:
            topics = [f"{KAFKA_TOPIC_PREFIX}_{camera_id}" for camera_id in camera_ids]
        else:
            print(f"Listening to all topics with prefix: {KAFKA_TOPIC_PREFIX}_*")
            print("Please specify camera IDs to listen to, or provide a list of known topics.")
            return
        
        # Subscribe to topics
        consumer.subscribe(topics)
        print(f"Subscribed to topics: {topics}")
        
        print("Starting to consume messages. Press Ctrl+C to stop.")
        
        while True:
            try:
                # Poll for messages
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        continue
                    else:
                        # Raise exception on error
                        raise KafkaException(msg.error())
                
                # Process the received message
                try:
                    # Decode the message value
                    message_value = msg.value().decode('utf-8')
                    
                    # Parse JSON if possible
                    try:
                        parsed_message = json.loads(message_value)
                        print(f"Received message from topic '{msg.topic()}' | "
                              f"partition {msg.partition()} | offset {msg.offset()}:")
                        print(json.dumps(parsed_message, indent=2, ensure_ascii=False))
                    except json.JSONDecodeError:
                        # If not JSON, print raw message
                        print(f"Received message from topic '{msg.topic()}' | "
                              f"partition {msg.partition()} | offset {msg.offset()}:")
                        print(f"Message: {message_value}")
                    
                    print("-" * 50)  # Separator for readability
                    
                except Exception as e:
                    print(f"Error processing message: {e}")

            except KafkaException as e:
                print(f"Kafka error: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")
    
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Close down consumer to commit final offsets.
        try:
            consumer.close()
        except Exception as e:
            print(f"Error closing consumer: {e}")
        print("Consumer closed.")

if __name__ == "__main__":
    # Example usage:
    # You can specify camera IDs to listen to specific cameras
    # Or modify this to listen to all topics with the prefix
    
    if len(sys.argv) > 1:
        # Get camera IDs from command line arguments
        camera_ids = sys.argv[1:]
        print(f"Listening to cameras: {camera_ids}")
        consume_messages(camera_ids)
    else:
        # Example: Listen to specific camera IDs
        # Replace with actual camera IDs you want to listen to
        example_camera_ids = ["330282000000010162"]  # Using the camera ID from your config
        print(f"No camera IDs provided, listening to example cameras: {example_camera_ids}")
        print("Usage: python test_consumer.py <camera_id1> <camera_id2> ...")
        consume_messages(example_camera_ids)