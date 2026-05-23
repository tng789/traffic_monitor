from ultralytics import YOLO
import cv2
import numpy as np
import redis
import json
from tell_time import recognize_timestamp_cv2,recognize_timestamp_easyocr
from pathlib import Path
import time
import easyocr
from fastapi import FastAPI, Query
from typing import Optional
import asyncio
import threading

# Global variables to store pre-loaded models
yolo_model = None
easyocr_reader = None
configurations = {}
active_processes = {}  # Dictionary to keep track of active video tracking processes

def is_red_light_on(img, position):
    """
    判断交通灯中的红灯是否亮起
    
    Args:
        img: cv2图像
        x1, y1: 区域左上角坐标
        x2, y2: 区域右下角坐标
    
    Returns:
        bool: True表示红灯亮，False表示红灯未亮
    """
    # 提取感兴趣区域
    x1, y1, x2, y2 = position
    roi = img[int(y1):int(y2), int(x1):int(x2)]
    
    # 转换为HSV色彩空间，便于颜色检测
    hsv_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
    
    # 定义红色范围（HSV空间）
    # 注意：红色在HSV中有两个范围，因为HSV是一个圆柱坐标系
    lower_red1 = np.array([0, 50, 50])
    upper_red1 = np.array([10, 255, 255])
    mask1 = cv2.inRange(hsv_roi, lower_red1, upper_red1)
    
    lower_red2 = np.array([170, 50, 50])
    upper_red2 = np.array([180, 255, 255])
    mask2 = cv2.inRange(hsv_roi, lower_red2, upper_red2)
    
    # 合并两个掩码
    red_mask = cv2.add(mask1, mask2)
    
    # 计算红色区域占总区域的比例
    height, width = red_mask.shape
    total_pixels = height * width
    red_pixels = cv2.countNonZero(red_mask)
    
    # 设置阈值，如果红色像素占比超过此阈值，则认为红灯亮起
    # 这个阈值可能需要根据实际情况调整
    red_ratio = red_pixels / total_pixels if total_pixels > 0 else 0
    
    # 返回判断结果，这里使用0.1作为阈值，即10%的区域为红色就认为灯亮了
    # 可以根据实际测试情况调整这个阈值
    return red_ratio > 0.1


def is_red_light_on_by_brightness(img, position, brightness_threshold=100):
    """
    通过亮度判断交通灯中的红灯是否亮起
    此方法首先检测交通灯区域的整体亮度，然后在高亮区域中查找红色像素
    
    Args:
        img: cv2图像
        x1, y1: 区域左上角坐标
        x2, y2: 区域右下角坐标
        brightness_threshold: 亮度阈值，用于判断灯是否亮起
    
    Returns:
        bool: True表示红灯亮，False表示红灯未亮
    """
    # 提取感兴趣区域
    x1, y1, x2, y2 = position
    roi = img[int(y1):int(y2), int(x1):int(x2)]
    roi = img[int(y1):int(y2), int(x1):int(x2)]
    
    # 转换为灰度图以测量亮度
    gray_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    
    # 计算平均亮度
    avg_brightness = cv2.mean(gray_roi)[0]
    
    # 如果整体亮度低于阈值，说明灯可能是灭的
    if avg_brightness < brightness_threshold:
        return False
    
    # 如果亮度足够高，进一步确认是否为红色
    hsv_roi = cv2.cvtColor(roi, cv2.COLOR_BGR2HSV)
    
    # 定义红色范围（HSV空间）
    lower_red1 = np.array([0, 50, 50])
    upper_red1 = np.array([10, 255, 255])
    mask1 = cv2.inRange(hsv_roi, lower_red1, upper_red1)
    
    lower_red2 = np.array([170, 50, 50])
    upper_red2 = np.array([180, 255, 255])
    mask2 = cv2.inRange(hsv_roi, lower_red2, upper_red2)
    
    # 合并两个掩码
    red_mask = cv2.add(mask1, mask2)
    
    # 计算红色区域占总区域的比例
    height, width = red_mask.shape
    total_pixels = height * width
    red_pixels = cv2.countNonZero(red_mask)
    
    # 如果红色像素比例超过一定阈值（比如10%），则认为是红灯亮起
    red_ratio = red_pixels / total_pixels if total_pixels > 0 else 0
    
    return red_ratio > 0.1


def load_models():
    """Function to pre-load both YOLO and EasyOCR models"""
    global yolo_model, easyocr_reader
    
    print("正在装入检测模型...")
    yolo_model = YOLO('best.pt')
    
    print("正在装入OCR模型...")
    easyocr_reader = easyocr.Reader(['en'], gpu=True,
                               model_storage_directory="./models",
                               download_enabled=False)  # 禁止下载，使用本地模型
    print("模型装载完成！")


def track_video(camera, stop_event=None, device='cuda'):
    """
    使用YOLO模型对视频进行目标跟踪，并将结果保存到Redis。

    Args:
        camera (str): Camera identifier string
        stop_event (threading.Event): Event to signal the function to stop
        device (str): 设备类型 ('cpu', 'cuda', 'cuda:0', 'cuda:1'等)
    """
    cfg_file = f'config/{camera}.json'
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
    
    frame_num = 0
    print(f"开始处理视频 for camera {camera}...")
    
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
        if frame_num % 15 != 0:
            continue

        print(f"已处理 {frame_num} 帧 for camera {camera}")
        
        timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, config['timestamp'][0], config['timestamp'][1])

        red_light = is_red_light_on_by_brightness(frame, config['light'])

        # 4. 对当前帧进行跟踪
        # persist=True 确保目标ID在帧间保持一致
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

                    # 将检测数据添加到Redis列表中
                    redis_connection.lpush(f'video:{camera}', json.dumps(detection_data))
                    
                    # 同时保留到lines数组中（为了兼容旧代码，但可以移除）
                    line = f"{frame_num}, {id}, {x1:.2f}, {y1:.2f}, {x2:.2f}, {y2:.2f}, {class_id}, {confidence:.2f}\n"
                    print(line)

            # 可选：在控制台打印进度
            # if frame_num % 30 == 0: # 每30帧打印一次
            #     print(f"已处理帧: {frame_num} / {total_frames}")

    # 6. 释放资源
    cap.release()
    print(f"视频处理完成 for camera {camera}")


# Create FastAPI app instance
app = FastAPI(title="Video Processing API", description="API for video processing with YOLO and EasyOCR")


@app.on_event('startup')
async def startup_event():
    """Load models when the application starts"""
    print("正在启动服务并装载模型...")
    # Models are now loaded before the server starts, so this is kept for compatibility
    # with other startup events if needed in the future
    print("服务启动完成！")


@app.get("/")
async def root():
    """Root endpoint to verify the service is running"""
    return {"message": "Video Processing API is running!"}


@app.get("/control")
async def control_camera(camera: str = Query(..., description="Camera identifier string"), 
                        command: str = Query(..., regex="^(start|stop)$", description="Command: 'start' or 'stop'")):
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