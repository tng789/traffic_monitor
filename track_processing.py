# from confluent_kafka import Consumer, KafkaException, KafkaError
import json
from collections import deque
# from config import  KAFKA_TOPIC_PREFIX
# import threading
# import time

# import sys

class Track:
    """
    Class to represent a track with a collection of detection data
    """
    def __init__(self, track_id):
        self.track_id = track_id
        self.tracks = deque()  # Use deque for efficient append/pop from both ends
    
    def add_data(self, data):
        """
        Add detection data to the track
        """
        self.tracks.append(data)
    
    def get_last_frame_id(self):
        """
        Get the frame_id of the last data in the track
        """
        if self.tracks:
            return self.tracks[-1]['frame_id']
        return None
    
    def handle(self):
        """
        Handle the track when frame_id difference exceeds threshold.
        This is a placeholder that will be implemented later.
        """
        print(f"Handling track {self.track_id} with {len(self.tracks)} data points")
        # Placeholder implementation - will be filled in later
        pass

class track_processor:
    """
    Kafka consumer that manages Track objects based on incoming data
    """
    def __init__(self, camera_id=None, group_id='track_consumer_group'):
        self.camera_id = camera_id
        self.current_tracks = {}  # Dictionary to hold track_id -> Track object mapping
        self.frame_diff_threshold = 60  # Frame difference threshold
        
        # Consumer configuration
#        consumer_config = {
#            # 'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#            'bootstrap.servers': "192.168.1.142:9092",
#            'group.id': self.group_id,
#            'auto.offset.reset': 'latest',  # Start from latest messages
#            'enable.auto.commit': True,     # Auto-commit offsets to mark messages as processed
#            'auto.commit.interval.ms': 1000 # Commit every 1 second
#        }
#        
#        # Create consumer
#        self.consumer = Consumer(consumer_config)
#        
#        # Determine topics to subscribe to
#        if camera_id:
#            topics = [f"{KAFKA_TOPIC_PREFIX}_{camera_id}"]  # Make sure it's a list
#        else:
#            # If no specific cameras provided, subscribe to all topics with the prefix pattern
#            topics = [f"{KAFKA_TOPIC_PREFIX}_*"]
#        
#        # Subscribe to topics
#        self.consumer.subscribe(topics)
#        print(f"Subscribed to topics: {topics}")
    
    def process_message(self, rmq_data):
        """
        Process incoming message data and manage track objects
        每一条新数据进来
        """
        try:
            data = json.loads(rmq_data)
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from message: {rmq_data}")
            return

        track_id = data['track_id']
        frame_id = data['frame_id']
        
        if track_id not in self.current_tracks:
            # Create new track object if this is a new track_id
            new_track = Track(track_id)
            new_track.add_data(data)
            self.current_tracks[track_id] = new_track
            print(f"Created new track for track_id: {track_id}")
        else:
            # Add data to existing track object
            track_obj = self.current_tracks[track_id]
            track_obj.add_data(data)
            
            # Check if frame_id difference exceeds threshold
            last_frame_id = track_obj.get_last_frame_id()
            if last_frame_id is not None and abs(frame_id - last_frame_id) > self.frame_diff_threshold:
                # Call handle function
                track_obj.handle()
                
                # Remove the track object from the dictionary
                del self.current_tracks[track_id]
                print(f"Processed and removed track {track_id}")
    
#    def run(self):
#        """
#        Main loop to consume messages and manage tracks
#        """
#        print("Starting Kafka track consumer. Press Ctrl+C to stop.")
#        
#        try:
#            while True:
#                # Poll for messages
#                msg = self.consumer.poll(timeout=1.0)  # Wait for 1 second for a message
#                
#                if msg is None:
#                    continue  # Timeout, no message received
#                
#                if msg.error():
#                    # Handle errors
#                    if msg.error().code() == KafkaError._PARTITION_EOF:
#                        # End of partition event
#                        continue
#                    else:
#                        # Other error
#                        print(f"Error: {msg.error()}")
#                        continue
#                
#                # Process the received message
#                try:
#                    # Decode the message value
#                    message_value = msg.value().decode('utf-8')
#                    
#                    # Parse JSON
#                    data = json.loads(message_value)
#                    
#                    # Process the message data
#                    self.process_message(data)
#                
#                except json.JSONDecodeError:
#                    print(f"Failed to decode JSON from message: {msg.value().decode('utf-8')}")
#                except KeyError as e:
#                    print(f"Missing expected key in message data: {e}")
#                    print(f"Message: {msg.value().decode('utf-8')}")
#                except Exception as e:
#                    print(f"Error processing message: {e}")
#        
#        except KeyboardInterrupt:
#            print("\nStopping consumer...")
#        finally:
#            # Close consumer
#            self.consumer.close()
#            print("Consumer closed.")
#
#def main():
#    """
#    Main function to run the Kafka track consumer
#    """
#    # Example usage - you can specify camera IDs to listen to specific cameras
#    # or leave as None to listen to all topics with the prefix
#    # camera_ids = None  # Change this to a list of specific camera IDs if needed
#    
#    if len(sys.argv) > 1:
#        camera_id = [sys.argv[1]]
#    else:
#        print("Usage: python kafka_track_consumer.py <camera_id>")
#
#    camera_id = sys.argv[1]
#    consumer = KafkaTrackConsumer(camera_id)
#    consumer.run()
#
#if __name__ == "__main__":
#    main()