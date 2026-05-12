import redis
import json
import time
import threading
from collections import defaultdict
from typing import List, Tuple
import sqlite3

class RedisDataRetriever:
    def __init__(self, video_name, redis_host='localhost', redis_port=6379, redis_db=0, db_path='violations.db'):
        """
        初始化Redis连接和SQLite数据库
        """
        self.video_name = video_name
        self.table_name = f"video_{video_name.replace('-', '_').replace('.', '_')}"  # 表名规范化
        
        try:
            self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
            # 测试连接
            self.r.ping()
            print(f"已连接到Redis服务器: {redis_host}:{redis_port}, DB: {redis_db}")
        except Exception as e:
            print(f"错误: 无法连接到Redis服务器: {e}")
            raise
            
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """
        初始化SQLite数据库，为当前视频流创建违规记录表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建针对此视频流的违规记录表
        create_table_sql = f'''
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                frame_id INTEGER,
                timestamp TEXT,
                traffic_volume INTEGER,
                track_id INTEGER,
                x1 REAL,
                y1 REAL,
                x2 REAL,
                y2 REAL,
                red_light_status BOOLEAN,
                violation TEXT
            )
        '''
        cursor.execute(create_table_sql)
        
        conn.commit()
        conn.close()

    def insert_violation_record(self, frame_id, timestamp, traffic_volume, track_id, x1, y1, x2, y2, red_light_status, violation):
        """
        插入违规记录到数据库的指定表
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        insert_sql = f'''
            INSERT INTO {self.table_name} (
                frame_id, timestamp, traffic_volume, track_id, 
                x1, y1, x2, y2, red_light_status, violation
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(insert_sql, (frame_id, timestamp, traffic_volume, track_id, x1, y1, x2, y2, red_light_status, violation))
        
        conn.commit()
        conn.close()

    def get_all_video_data(self):
        """
        从Redis获取所有视频数据
        """
        # 获取所有数据并反转顺序（因为lrange返回的是最新的在前面）
        raw_data = self.r.lrange('video', 0, -1)
        detections = [json.loads(data) for data in raw_data]
        # 按帧号排序，确保是时间顺序
        detections.sort(key=lambda x: x['frame_id'])
        return detections

    def group_by_track_id(self, detections):
        """
        按track_id分组数据
        """
        track_groups = defaultdict(list)
        for detection in detections:
            track_groups[detection['track_id']].append(detection)
        return track_groups

    def is_track_completed(self, track_detections, current_frame, threshold=60):
        """
        判断某个track是否已完成（在current_frame-60到current_frame之间未出现）
        """
        # 获取该track出现的最高帧号
        max_frame = max(detection['frame_id'] for detection in track_detections)
        # 如果最大帧号小于当前帧减去阈值，则认为已完成
        return max_frame < (current_frame - threshold)

    def is_track_exceeds_duration(self, track_detections, max_duration=4500):
        """
        判断某个track是否超过了最大持续时间（4500帧）
        """
        # 获取该track的最小帧号和最大帧号
        min_frame = min(detection['frame_id'] for detection in track_detections)
        max_frame = max(detection['frame_id'] for detection in track_detections)
        duration = max_frame - min_frame
        return duration >= max_duration

    def should_process_track(self, track_detections, current_frame, frame_threshold=60, duration_threshold=4500):
        """
        根据规则判断是否应该处理某个track
        """
        # 规则1: 如果某个track id在M-60 ~ M帧范围内没有出现，则视为该id已经完结，取出待用
        if self.is_track_completed(track_detections, current_frame, frame_threshold):
            return True
        
        # 规则2: 如果某个track id在M-60~M帧范围内出现，但是整个事件长度超过5分钟（4500帧），则视为有效数据，取出待用
        if self.is_track_exceeds_duration(track_detections, duration_threshold):
            return True
            
        # 否则不处理
        return False

    def point_in_polygon(self, point: Tuple[float, float], polygon: List[Tuple[float, float]]) -> bool:
        """
        判断点是否在多边形内部
        :param point: (x, y) 待判断点
        :param polygon: [(x1, y1), (x2, y2), ...] 多边形顶点列表
        :return: True if point is inside the polygon
        """
        x, y = point
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

    def detect_retrograde(self, track_detections: List[dict], region_vertices: List[Tuple[float, float]], 
                         normal_direction: str) -> bool:
        """
        检测车辆是否逆行
        :param track_detections: 一个track的检测数据列表
        :param region_vertices: 区域顶点列表，形成一个多边形
        :param normal_direction: 正常行驶方向 ("up", "down", "left", "right")
        :return: True if retrograde detected
        """
        # 按帧排序
        sorted_detections = sorted(track_detections, key=lambda x: x['frame_id'])
        
        # 记录在区域内经过的点
        points_in_region = []
        for detection in sorted_detections:
            center_x = (detection['x1'] + detection['x2']) / 2
            center_y = (detection['y1'] + detection['y2']) / 2
            point = (center_x, center_y)
            
            if self.point_in_polygon(point, region_vertices):
                points_in_region.append((detection['frame_id'], point))
        
        # 如果在区域内只有一个点或没有点，无法判断运动方向
        if len(points_in_region) < 2:
            return False
        
        # 获取进入和离开区域的点
        points_in_region.sort(key=lambda x: x[0])  # 按帧排序
        entry_point = points_in_region[0][1]  # 进入区域时的位置
        exit_point = points_in_region[-1][1]  # 离开区域时的位置
        
        # 根据正常行驶方向判断是否逆行
        entry_x, entry_y = entry_point
        exit_x, exit_y = exit_point
        
        if normal_direction == "up":
            return exit_y > entry_y  # 正常应该是y坐标变小，如果变大则是逆行
        elif normal_direction == "down":
            return exit_y < entry_y  # 正常应该是y坐标变大，如果变小则是逆行
        elif normal_direction == "left":
            return exit_x > entry_x  # 正常应该是x坐标变小，如果变大则是逆行
        elif normal_direction == "right":
            return exit_x < entry_x  # 正常应该是x坐标变大，如果变小则是逆行
        else:
            print(f"未知的正常行驶方向: {normal_direction}")
            return False

    def detect_helmet_violation(self, track_detections: List[dict]) -> bool:
        """
        检测是否未佩戴头盔
        :param track_detections: 一个track的检测数据列表
        :return: True if helmet violation detected
        """
        total_count = len(track_detections)
        if total_count == 0:
            return False
            
        # 统计class_id为1的检测点数量（假设class_id=1代表未佩戴头盔的人）
        helmetless_count = sum(1 for detection in track_detections if detection.get('class_id') == 1)
        
        # 如果class_id为1的检测点数量超过整个轨迹的50%，则判断为不戴头盔
        return helmetless_count / total_count > 0.5

    def detect_carrying_violation(self, track_detections: List[dict]) -> bool:
        """
        检测是否骑车带人
        :param track_detections: 一个track的检测数据列表
        :return: True if carrying violation detected
        """
        total_count = len(track_detections)
        if total_count == 0:
            return False
            
        # 统计class_id为3的检测点数量（假设class_id=3代表骑车带人）
        carrying_count = sum(1 for detection in track_detections if detection.get('class_id') == 3)
        
        # 如果class_id为3的检测点数量超过整个轨迹的50%，则判断为骑车带人
        return carrying_count / total_count > 0.5

    def remove_processed_data_from_redis(self, processed_detections):
        """
        从Redis中删除已处理的数据
        """
        for detection in processed_detections:
            serialized_data = json.dumps(detection)
            # 从Redis列表中删除特定元素
            self.r.lrem('video', 1, serialized_data)

    def process_at_frame_interval(self, frame_interval=750, frame_threshold=60, duration_threshold=4500):
        """
        每隔frame_interval帧执行一次数据处理
        """
        while True:
            # 获取当前最大的帧号
            all_detections = self.get_all_video_data()
            if not all_detections:
                print("没有数据可处理，等待...")
                time.sleep(5)  # 等待5秒再检查
                continue
                
            current_max_frame = max(detection['frame_id'] for detection in all_detections)
            
            # 检查是否达到了处理间隔
            if current_max_frame % frame_interval != 0:
                print(f"当前帧 {current_max_frame} 未达到处理间隔 {frame_interval}，等待...")
                time.sleep(2)  # 等待2秒再检查
                continue
            
            print(f"[{self.video_name}] 处理帧间隔: 当前帧 {current_max_frame}")
            
            # 按track_id分组
            track_groups = self.group_by_track_id(all_detections)
            
            # 存储需要处理的检测数据
            to_process = []
            
            # 遍历每个track，根据规则判断是否处理
            for track_id, detections in track_groups.items():
                if self.should_process_track(detections, current_max_frame, frame_threshold, duration_threshold):
                    print(f"[{self.video_name}] 处理Track ID {track_id}: 共 {len(detections)} 个检测点")
                    to_process.extend(detections)
            
            # 从Redis中删除已处理的数据
            if to_process:
                print(f"[{self.video_name}] 找到 {len(to_process)} 个需要处理的检测数据，从Redis中删除...")
                self.remove_processed_data_from_redis(to_process)
                
                # 这里可以根据需要进一步处理数据
                # 例如保存到文件，发送到其他服务等
                self.handle_processed_data(to_process)
            else:
                print(f"[{self.video_name}] 没有需要处理的数据")
            
            # 等待一段时间后再进行下次检查, 这时间待定
            time.sleep(2)

    def handle_processed_data(self, detections):
        """
        处理已提取的数据，可以在这里实现具体的数据处理逻辑
        """
        # 示例：按track_id分组并打印统计信息
        track_groups = self.group_by_track_id(detections)
        
        # 获取当前批次的最大track_id，对应traffic_volume字段
        max_track_id = max(track_groups.keys()) if track_groups else 0
        
        print(f"[{self.video_name}] 处理 {len(track_groups)} 个不同的track，共 {len(detections)} 个检测点:")
        for track_id, track_detections in track_groups.items():
            min_frame = min(d['frame_id'] for d in track_detections)
            max_frame = max(d['frame_id'] for d in track_detections)
            duration = max_frame - min_frame
            print(f"  [{self.video_name}] Track {track_id}: 帧范围 {min_frame}-{max_frame}, 持续 {duration} 帧")
            
            # 获取轨迹的起始坐标点
            first_detection = track_detections[0]
            x1_start = first_detection['x1']
            y1_start = first_detection['y1']
            x2_start = first_detection['x2']
            y2_start = first_detection['y2']
            
            # 获取时间戳（使用第一个检测点的时间戳）
            timestamp = first_detection.get('timestamp', '')
            
            # 添加判断逆行的功能
            # 方法：事先设定判断区域, 由多个坐标作为顶点组成的凸多边形，并指定该区域的正常行驶方向（向上或向下）。 
            #      如果 track_id的移动轨迹穿过该区域，但与设定的正常行驶方向相反，则判定为逆行。
            #      否则，认为是正常行驶。
            #      打印判断结果，逆行 或 正常行驶
            
            # 定义一个示例区域（可以根据实际需求调整）
            # 这里假设是一个矩形区域，从左上角到右下角
            region_vertices = [
                (300, 200),   # 左上角
                (500, 200),   # 右上角
                (500, 400),   # 右下角
                (300, 400)    # 左下角
            ]
            normal_direction = "down"  # 假设正常行驶方向是从上到下
            
            is_retrograde = self.detect_retrograde(track_detections, region_vertices, normal_direction)
            
            if is_retrograde:
                print(f"  [{self.video_name}] Track {track_id}: 逆行")
                # 插入数据库记录
                self.insert_violation_record(
                    frame_id=min_frame,
                    timestamp=timestamp,
                    traffic_volume=max_track_id,
                    track_id=track_id,
                    x1=x1_start,
                    y1=y1_start,
                    x2=x2_start,
                    y2=y2_start,
                    red_light_status=first_detection.get('red_light_status', False),
                    violation='逆行'
                )
            else:
                print(f"  [{self.video_name}] Track {track_id}: 正常行驶")

            # 添加判断闯红灯功能
            # 方法：事先设闯红灯定判断区域, 由多个坐标作为顶点组成的凸多边形。 
            #      在红灯亮起期间，track_id在该区域内至少出现3次 ，则判定为闯红灯。 否则为正常行驶
            #      打印判断结果，闯红灯 或 正常行驶
            
            # 定义闯红灯检测区域（可以根据实际需求调整）
            violation_region = [
                (300, 200),   # 左上角
                (500, 200),   # 右上角
                (500, 400),   # 右下角
                (300, 400)    # 左下角
            ]
            
            # 检查是否在红灯期间闯入违规区域
            violations_count = 0
            for detection in track_detections:
                # 检查是否在红灯亮起时出现在违规区域内
                if detection.get('red_light_status', False):  # 假设extract.py写入了红灯状态
                    center_x = (detection['x1'] + detection['x2']) / 2
                    center_y = (detection['y1'] + detection['y2']) / 2
                    point = (center_x, center_y)
                    
                    if self.point_in_polygon(point, violation_region):
                        violations_count += 1
                        
            # 如果在红灯期间至少出现了3次在违规区域内，则判断为闯红灯
            if violations_count >= 3:
                print(f"  [{self.video_name}] Track {track_id}: 闯红灯")
                # 插入数据库记录
                self.insert_violation_record(
                    frame_id=min_frame,
                    timestamp=timestamp,
                    traffic_volume=max_track_id,
                    track_id=track_id,
                    x1=x1_start,
                    y1=y1_start,
                    x2=x2_start,
                    y2=y2_start,
                    red_light_status=True,
                    violation='闯红灯'
                )
            else:
                print(f"  [{self.video_name}] Track {track_id}: 正常行驶")

            # 判断不戴头盔
            is_helmet_violation = self.detect_helmet_violation(track_detections)
            if is_helmet_violation:
                print(f"  [{self.video_name}] Track {track_id}: 不戴头盔")
                # 插入数据库记录
                self.insert_violation_record(
                    frame_id=min_frame,
                    timestamp=timestamp,
                    traffic_volume=max_track_id,
                    track_id=track_id,
                    x1=x1_start,
                    y1=y1_start,
                    x2=x2_start,
                    y2=y2_start,
                    red_light_status=first_detection.get('red_light_status', False),
                    violation='不戴头盔'
                )
            else:
                print(f"  [{self.video_name}] Track {track_id}: 正常行驶")

            # 判断骑车带人
            is_carrying_violation = self.detect_carrying_violation(track_detections)
            if is_carrying_violation:
                print(f"  [{self.video_name}] Track {track_id}: 骑车带人")
                # 插入数据库记录
                self.insert_violation_record(
                    frame_id=min_frame,
                    timestamp=timestamp,
                    traffic_volume=max_track_id,
                    track_id=track_id,
                    x1=x1_start,
                    y1=y1_start,
                    x2=x2_start,
                    y2=y2_start,
                    red_light_status=first_detection.get('red_light_status', False),
                    violation='骑车带人'
                )
            else:
                print(f"  [{self.video_name}] Track {track_id}: 正常行驶")


def main():
    """
    主函数，启动Redis数据处理
    """
    # 假设我们有一个视频名称，实际使用时应该通过参数传入
    import sys
    if len(sys.argv) < 2:
        print("请提供视频名称作为参数")
        return
    
    video_name = sys.argv[1]
    retriever = RedisDataRetriever(video_name)
    
    print(f"[{video_name}] 开始监控Redis数据...")
    print("处理规则:")
    print("1. 每隔750帧处理一次数据")
    print("2. 如果track在M-60到M帧内未出现，则视为结束，提取其数据")
    print("3. 如果track持续时间超过4500帧(5分钟)，则提取其数据")
    print("4. 提取后从Redis删除已处理的数据")
    
    # 开始处理
    retriever.process_at_frame_interval()


if __name__ == "__main__":
    main()