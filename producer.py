from ultralytics import YOLO
import cv2
import numpy as np
import redis
import json
from tell_time import recognize_timestamp_easyocr
# from pathlib import Path
from datetime import datetime, timedelta
import easyocr
from fastapi import FastAPI, Query 
# from typing import Optional, Annotated
# import asyncio
import threading

import time
from confluent_kafka import Producer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_PREFIX, PRODUCER_ACKS

from fixed_fifo import FixedFIFO

# Global variables to store pre-loaded models
yolo_model = None
easyocr_reader = None
configurations = {}
active_processes = {}  # Dictionary to keep track of active video tracking processes

class CameraDataProducer:
    def __init__(self, camera_id: str):
        self.camera_id = camera_id
        self.topic = f"{KAFKA_TOPIC_PREFIX}_{camera_id}"
        self.producer = Producer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'acks': PRODUCER_ACKS,
        })
    
    def send_data(self, data_id: str, timestamp: float = None):
        """发送一条数据"""
        if timestamp is None:
            timestamp = time.time()
        
        message = {
            'camera': self.camera_id,
            'id': data_id,
            'timestamp': timestamp,
        }
        
        # 按 camera_id 作为 key，确保同一 camera 的数据进入同一分区
        self.producer.produce(
            topic=self.topic,
            key=self.camera_id,
            value=json.dumps(message),
        )
        self.producer.poll(0)  # 触发回调
    
    def flush(self):
        """确保所有消息已发送"""
        self.producer.flush()
def lit(img):
    '''tell that if the light at the specific area(img) are lit
    '''
    #img, opened by cv2.imread
    frame = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, bw = cv2.threshold(frame,127,255,cv2.THRESH_BINARY)

    height, width, _ = img.shape
    whites = np.count_nonzero(bw==255)
    # print(f"{whites=} area={height*width}")
    if whites * 4 > height * width:
        return True
    else:
        return False

def load_models():
    """Function to pre-load both YOLO and EasyOCR models"""
    global yolo_model, easyocr_reader
    
    print("正在装入检测模型...")
    yolo_model = YOLO('./models/best.pt')
    
    print("正在装入OCR模型...")
    easyocr_reader = easyocr.Reader(['en'], gpu=True,
                               model_storage_directory="./models",
                               download_enabled=False)  # 禁止下载，使用本地模型
    print("模型装载完成！")

def track_video(camera, stop_event=None, device='cuda', pace = 3):
    """
    使用YOLO模型对视频进行目标跟踪，并将结果保存到Redis。

    Args:
        camera (str): Camera identifier string
        stop_event (threading.Event): Event to signal the function to stop
        device (str): 设备类型 ('cpu', 'cuda', 'cuda:0', 'cuda:1'等)
    """
    cfg_file = f'{camera}.json'
    try:
        with open(cfg_file) as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"错误: 找不到配置文件 {cfg_file}")
        return

    # 2. 打开视频文件
    cap = cv2.VideoCapture(config['source'])
    if not cap.isOpened():
        print(f"错误: 无法打开视频文件 {config['source']}")
        return

    # 获取视频信息
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    print(f"视频信息: 总帧数={total_frames}, FPS={fps:.2f}")

    global yolo_model, redis_connection
    # 清空之前可能存在的video相关的数据
    redis_connection.delete(f'video:{camera}')
    
    print(f"开始预处理视频 for camera {camera}...")

    timestamp = None
    redlight_queue = FixedFIFO(maxlen=(fps/pace)//2 + 1)
    
    while True:
        ret, frame = cap.read()
        if not ret:
            print(f"视频处理完毕 或者 处理视频出错 {camera}")
            break
        timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, config['timestamp'][0], config['timestamp'][1])
        red_light_area = frame[config['light'][1]:config['light'][3], config['light'][0]:config['light'][2]]
        red_light = lit(red_light_area)
        redlight_queue.push(red_light)

        if  timestamp is not None and redlight_queue.is_full():
            break
    
    print(f"开始处理视频 for camera {camera}...")
    frame_num = 0
    interval = 1000 // fps * pace
    std_timestamp = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_frame_timestamp_obj = std_timestamp

    producer = CameraDataProducer(camera)

    # lines = []
    while True:
        # Check if stop event was triggered
        if stop_event and stop_event.is_set():
            print(f"停止信号接收到，结束处理视频 for camera {camera}")
            break
        
        ret, frame = cap.read()
        if not ret:
            print(f"视频处理完毕 for camera {camera}")
            break
        
        frame_num += 1
        if frame_num % pace != 0:             # 每隔pace帧处理一次,这个地方要详细考虑一下，当前测试视频并未到每秒25帧。
            continue
        
        timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, config['timestamp'][0], config['timestamp'][1])
        # print(f"帧 {frame_num} 识别到的时间戳: {timestamp}")

        if timestamp is None :
            if last_frame_timestamp_obj == std_timestamp:
                continue
            else:
                timestamp_obj = last_frame_timestamp_obj + timedelta(milliseconds=interval)
                timestamp = timestamp_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                
        last_frame_timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

        
        red_light_area = frame[config['light'][1]:config['light'][3], config['light'][0]:config['light'][2]]
        red_light = lit(red_light_area)

        redlight_queue.push(red_light)

        if not red_light: # 如果当前帧没有红灯，则检查上一帧和前两帧是否有红灯
            if redlight_queue.get(-2) or redlight_queue.get(-3):
                red_light = True
        

        # 4. 对当前帧进行跟踪
        results = yolo_model.track(source=frame, persist=True, verbose=False)

        # 5. 解析并写入跟踪结果
        for result in results:
            # 检查是否有跟踪框
            if result.boxes is not None and result.boxes.id is not None:
                boxes = result.boxes
                # 获取边界框坐标 (x1, y1, x2, y2), 置信度, 类别ID, 跟踪ID
                # xyxy 是左上角和右下角坐标
                xyxy = boxes.xyxy.cpu().numpy()
                conf = boxes.conf.cpu().numpy()
                cls = boxes.cls.cpu().numpy()
                track_id = boxes.id.cpu().numpy()

                for i in range(len(xyxy)):
                    x1, y1, x2, y2 = xyxy[i]
                    confidence = conf[i]
                    class_id = int(cls[i])
                    id = int(track_id[i])
                    
                    # 创建数据对象
                    detection_data = {
                        'frame_id': int(frame_num),
                        'track_id': int(id),
                        'x1': float(x1),
                        'y1': float(y1),
                        'x2': float(x2),
                        'y2': float(y2),
                        'class_id': int(class_id),
                        'confidence': float(confidence),
                        'red_light': red_light,
                        'timestamp': timestamp,
                        'camera': camera
                    }

                    # 将检测数据添加到Redis列表中, 
                    # redis_connection.lpush(f'video:{camera}', json.dumps(detection_data))
                    
                    # 同时保留到lines数组中（为了兼容旧代码，但可以移除）
                    # line = f"{frame_num}, {id}, {x1:.2f}, {y1:.2f}, {x2:.2f}, {y2:.2f}, {class_id}, {confidence:.2f}\n"
                            # 按 camera_id 作为 key，确保同一 camera 的数据进入同一分区
                    producer.produce(topic=camera, key=camera, value = json.dumps(detection_data))
                    
                    producer.poll(0)  # 触发回调
                    
                    # line = json.dumps(detection_data)
                    # print(line)
                    # lines.append(line)

            # 可选：在控制台打印进度
        if frame_num % 30 == 0: # 每30帧打印一次
            print(f"已处理帧: {frame_num} / {total_frames}")

        # last_frame_timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    # 6. 释放资源
    cap.release()
    print(f"视频处理完成 for camera {camera}")
    # 把lines保存到文件 
    # with open(f'time_redlight_{camera}.txt', 'w') as f:
        # f.writelines(lines)


# Create FastAPI app instance with lifespan
app = FastAPI(
    title="Video Processing API", 
    description="API for video processing with YOLO and EasyOCR"
)


@app.get("/")
async def root():
    """Root endpoint to verify the service is running"""
    return {"message": "Video Processing API is running!"}


@app.get("/control")
async def control_camera(camera: str = Query(..., description="Camera identifier string"), 
                         command: str = Query(..., pattern="^(start|stop)$", description="Command: 'start' or 'stop'")):
    """
    Control endpoint to handle camera commands.
    
    Args:
        camera (str): Camera identifier string
        command (str): Command - either 'start' or 'stop'
    
    Returns:
        dict: Response with the action taken
    """
    global active_processes
    
    # Process the command
    if command == "start":
        print(f"Starting processing for camera {camera}")

        # Check if the camera is already being processed
        if camera in active_processes:
            if active_processes[camera]['running']:
                return {
                    "camera": camera,
                    "command": command,
                    "status": "already_running",
                    "message": f"Camera {camera} is already being processed"
                }
        
        # Create a stop event for this camera
        stop_event = threading.Event()
        
        # Create a thread for video processing
        video_thread = threading.Thread(
            target=track_video, 
            args=(camera, stop_event)
        )
        
        # Store the thread and stop event
        active_processes[camera] = {
            'thread': video_thread,
            'stop_event': stop_event,
            'running': True
        }
        
        # Start the video processing thread
        video_thread.start()
        
        return {
            "camera": camera,
            "command": command,
            "status": "processing_started",
            "message": f"Started processing for camera {camera}"
        }
    elif command == "stop":
        print(f"Stopping processing for camera {camera}")
        
        # Check if the camera is currently being processed
        if camera not in active_processes or not active_processes[camera]['running']:
            return {
                "camera": camera,
                "command": command,
                "status": "not_running",
                "message": f"Camera {camera} is not currently being processed"
            }
        
        # Set the stop event to signal the thread to stop
        active_processes[camera]['stop_event'].set()
        
        # Optionally wait for the thread to finish (with timeout)
        # active_processes[camera]['thread'].join(timeout=5)  # Wait up to 5 seconds
        
        # Mark as not running anymore
        active_processes[camera]['running'] = False
        
        return {
            "camera": camera,
            "command": command,
            "status": "processing_stopped", 
            "message": f"Stop signal sent for camera {camera}"
        }
    else:
        return {"error": "Invalid command. Use 'start' or 'stop'"}


# --- Main program for backward compatibility ---
if __name__ == "__main__":
    # 连接到Redis
    try:
        redis_host = "localhost"
        redis_port = 6379
        redis_db = 0
        global redis_connection
        redis_connection = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        # 测试连接
        # redis_connection.ping()
        print(f"已连接到Redis服务器: {redis_host}:{redis_port}, DB: {redis_db}")
    except Exception as e:
        print(f"错误: 无法连接到Redis服务器: {e}")
        exit()

    # Load models before starting the server
    print("正在启动服务并装载模型...")
    load_models()
    print("模型装载完成！")

    # device = 'cuda'
        
        # Run tracking function
        # track_video(input_video, yolo_model, config = confs,  device=device)

    # Start the FastAPI server if no arguments provided
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)