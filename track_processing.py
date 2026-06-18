import json
from collections import deque

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
        Analyzes the track data to detect violations and writes results to SQLite database.
        """
        if not self.tracks:
            return
            
        # Convert deque to list for easier processing
        track_data_list = list(self.tracks)
        
        # Get camera ID from the first data point
        camera_id = track_data_list[0]['camera']
        violation_records = []
        
        # 1. Determine the class_id that appears most frequently (excluding class_id 0 as normal behavior)
        class_counts = {}
        for data_point in track_data_list:
            class_id = data_point['class_id']
            if class_id != 0:  # Exclude class_id 0 (normal behavior)
                class_counts[class_id] = class_counts.get(class_id, 0) + 1
        
        # Find the most frequent non-zero class_id
        if class_counts:
            most_frequent_class_id = max(class_counts, key=class_counts.get)
            # Map class_id to violation type
            class_id_to_violation = {
                1: 1,  # 单人不戴头盔
                2: 2,  # 双人戴头盔
                3: 3   # 双人不戴头盔
            }
            if most_frequent_class_id in class_id_to_violation:
                violation_records.append({
                    'type': class_id_to_violation[most_frequent_class_id],
                    'first_data': track_data_list[0]
                })
        
        # Read configuration file for this camera
        config_file = f'{camera_id}.json'
        try:
            with open(config_file) as f:
                config = json.load(f)
        except FileNotFoundError:
            print(f"Configuration file {config_file} not found.")
            return
        
        # 2. Check for wrong direction
        if 'wrongdirection' in config:
            wrong_direction_config = config['wrongdirection']
            direction = wrong_direction_config['direction']
            area_coords = wrong_direction_config['area']
            
            if self.check_wrong_direction(track_data_list, area_coords, direction):
                violation_records.append({
                    'type': 4,  # 逆行
                    'first_data': track_data_list[0]
                })
        
        # 3. Check for running red light
        if 'runredlight' in config:
            red_light_area = config['runredlight']
            
            if self.check_run_red_light(track_data_list, red_light_area):
                violation_records.append({
                    'type': 5,  # 闯红灯
                    'first_data': track_data_list[0]
                })
        
        # Write violations to SQLite database
        if violation_records:
            self.write_violations_to_db(camera_id, violation_records)
    
    def check_wrong_direction(self, track_data_list, area_coords, direction):
        """
        Check if the track violates the wrong direction rule.
        :param track_data_list: List of track data points
        :param area_coords: Coordinates of the area where direction matters
        :param direction: Normal direction ('up' or 'down')
        :return: True if wrong direction is detected, False otherwise
        """
        # Filter track points that are inside the area
        points_in_area = []
        for data_point in track_data_list:
            x_center = (data_point['x1'] + data_point['x2']) / 2
            y_center = (data_point['y1'] + data_point['y2']) / 2
            
            if self.point_in_polygon(x_center, y_center, area_coords):
                points_in_area.append((x_center, y_center, data_point['frame_id']))
        
        if len(points_in_area) < 2:
            return False  # Need at least 2 points to determine direction
        
        # Sort by frame_id to get chronological order
        points_in_area.sort(key=lambda x: x[2])
        
        # Get first and last points in the area
        first_x, first_y, _ = points_in_area[0]
        last_x, last_y, _ = points_in_area[-1]
        
        # Calculate movement distances
        dx = abs(last_x - first_x)
        dy = abs(last_y - first_y)
        
        # Determine primary movement direction
        if dx > dy:
            # Movement is primarily horizontal
            return False  # Wrong direction only applies to vertical movement
        else:
            # Movement is primarily vertical
            if direction == 'up':
                # Normal direction is up, so if last_y < first_y (moving up), it's normal
                # If last_y > first_y (moving down), it's wrong direction
                return last_y > first_y
            elif direction == 'down':
                # Normal direction is down, so if last_y > first_y (moving down), it's normal
                # If last_y < first_y (moving up), it's wrong direction
                return last_y < first_y
        
        return False
    
    def check_run_red_light(self, track_data_list, red_light_area):
        """
        Check if the track runs a red light.
        :param track_data_list: List of track data points
        :param red_light_area: Coordinates of the red light area
        :return: True if running red light is detected, False otherwise
        """
        # Filter track points that are inside the red light area when red light is on
        points_in_red_light_area = []
        for data_point in track_data_list:
            if data_point['red_light']:  # Red light is on
                # x_center = (data_point['x1'] + data_point['x2']) / 2
                # y_center = (data_point['y1'] + data_point['y2']) / 2
                
                x = (data_point['x1'] + data_point['x2']) / 2
                y = data_point['y2']
                if self.point_in_polygon(x, y, red_light_area):
                    points_in_red_light_area.append((x, y, data_point['frame_id']))
        
        if len(points_in_red_light_area) < 2:
            return False  # Need at least 2 points to determine direction
        
        # Sort by frame_id to get chronological order
        points_in_red_light_area.sort(key=lambda x: x[2])
        
        # Get first and last points in the red light area
        first_x, first_y, _ = points_in_red_light_area[0]
        last_x, last_y, _ = points_in_red_light_area[-1]
        
        # Calculate movement distances
        dx = abs(last_x - first_x)
        dy = abs(last_y - first_y)
        
        # Check if the movement is primarily in the Y direction (vertical)
        # and if the vehicle moved upward (from lower Y to higher Y)
        if dy > dx:  # Primary movement is vertical
            # Vehicle moving upward (Y decreases as we move up in image coordinates)
            # So if last_y < first_y, it means the vehicle moved upward
            if last_y < first_y:
                return True
        
        return False
    
    def point_in_polygon(self, x, y, polygon):
        """
        Check if a point is inside a polygon using ray casting algorithm.
        :param x: X coordinate of the point
        :param y: Y coordinate of the point
        :param polygon: List of (x, y) coordinates of the polygon vertices
        :return: True if point is inside the polygon, False otherwise
        """
        n = len(polygon)
        inside = False
        
        p1x, p1y = polygon[0]
        for i in range(1, n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        
        return inside
    
    def write_violations_to_db(self, camera_id, violation_records):
        """
        Write violation records to SQLite database.
        :param camera_id: Camera ID for the table name
        :param violation_records: List of violation records to write
        """
        import sqlite3
        
        # Create table name based on camera_id
        table_name = f"video_{camera_id.replace('-', '_').replace('.', '_')}"
        
        # Connect to database and create table if not exists
        conn = sqlite3.connect('violations.db')
        cursor = conn.cursor()
        
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                track_id INTEGER,
                time TEXT,
                x1 REAL,
                y1 REAL,
                x2 REAL,
                y2 REAL,
                violation INTEGER
            )
        '''
        cursor.execute(create_table_sql)
        
        # Insert violation records
        for record in violation_records:
            first_data = record['first_data']
            insert_sql = f'''
                INSERT INTO {table_name} (
                    track_id, time, x1, y1, x2, y2, violation
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            '''
            cursor.execute(insert_sql, (
                self.track_id,
                first_data['timestamp'],
                first_data['x1'],
                first_data['y1'],
                first_data['x2'],
                first_data['y2'],
                record['type']
            ))
        
        conn.commit()
        conn.close()

class track_processor:
    """
    consumer that manages Track objects based on incoming data
    """
    def __init__(self, camera_id=None, group_id='track_consumer_group'):
        self.camera_id = camera_id
        self.current_tracks = {}  # Dictionary to hold track_id -> Track object mapping
        
        #ByteTrack默认30, 其实30次检测。如果每3帧取1帧的话， frame_diff应该是30*3
        self.frame_diff_threshold = 90  # Frame difference threshold
        
    
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
        
        # Add data to existing track object or create new one
        if track_id not in self.current_tracks:
            # Create new track object if this is a new track_id
            new_track = Track(track_id)
            self.current_tracks[track_id] = new_track
            print(f"Created new track for track_id: {track_id}")
        
        # Add data to the track (whether it's new or existing)
        track_obj = self.current_tracks[track_id]
        track_obj.add_data(data)
        
        # Now iterate through all tracks to check if any have exceeded the threshold
        tracks_to_remove = []
        for existing_track_id, existing_track_obj in self.current_tracks.items():
            last_frame_id = existing_track_obj.get_last_frame_id()
            # print(f"track_id={existing_track_id}, last_frame_id={last_frame_id}, current_frame_id={frame_id}")
            
            if last_frame_id is not None and abs(frame_id - last_frame_id) > self.frame_diff_threshold:
                print(f"Frame difference exceeded threshold for track {existing_track_id}")
                
                # 此处应该增加一个判断，如果existing_track_obj里面轨迹数据少于某个阈值，则不处理，并且将track_id加入待删除名单。
                if len(existing_track_obj.tracks) < 5:      #fps13, 每3帧取1帧，5相当于1秒多点。
                    print(f"Track {existing_track_id} has less than 5 points, not processing")
                    tracks_to_remove.append(existing_track_id)
                    continue

                # Call handle function for this track
                existing_track_obj.handle()
                
                # Mark this track for removal
                tracks_to_remove.append(existing_track_id)
        
        # Remove all marked tracks from current_tracks
        for track_id_to_remove in tracks_to_remove:
            del self.current_tracks[track_id_to_remove]
            print(f"Processed and removed track {track_id_to_remove}")
    