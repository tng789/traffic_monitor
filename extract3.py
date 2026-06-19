from ultralytics import YOLO
import cv2
import numpy as np
import json
from tell_time import recognize_timestamp_easyocr
from datetime import datetime, timedelta
import easyocr
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
import threading
import asyncio
from typing import Dict, List
from fastapi.responses import HTMLResponse

from rmq import Producer

from fixed_fifo import FixedFIFO

# Global variables to store pre-loaded models
yolo_model = None
easyocr_reader = None
configurations = {}
active_processes = {}  # Dictionary to keep track of active video tracking processes
process_logs = {}  # Dictionary to store logs for each camera
websocket_connections = {}  # Dictionary to store WebSocket connections for each camera

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
    yolo_model = YOLO('./models/best_v8s.pt')
    
    print("正在装入OCR模型...")
    easyocr_reader = easyocr.Reader(['en'], gpu=True,
                               model_storage_directory="./models",
                               download_enabled=False)  # 禁止下载，使用本地模型
    print("模型装载完成！")

def log_message(camera, message):
    """Log a message for a specific camera"""
    if camera not in process_logs:
        process_logs[camera] = []
    process_logs[camera].append({
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'message': message
    })
    
    # Keep only the last 100 messages to prevent memory overflow
    if len(process_logs[camera]) > 100:
        process_logs[camera] = process_logs[camera][-100:]
    
    # Send the message to connected WebSocket clients
    if camera in websocket_connections:
        for connection in websocket_connections[camera]:
            try:
                # We'll send the message to the WebSocket in the background
                asyncio.create_task(send_log_to_websocket(connection, {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'message': message
                }))
            except:
                # Remove broken connections
                if connection in websocket_connections[camera]:
                    websocket_connections[camera].remove(connection)

async def send_log_to_websocket(websocket, log_entry):
    """Send a log entry to a WebSocket connection"""
    try:
        await websocket.send_text(json.dumps(log_entry))
    except Exception as e:
        print(f"错误: 无法发送消息到 WebSocket: {e}")
        # pass  # Connection might be closed

def track_video(camera, stop_event=None, device='cuda', pace = 3):
    """
    使用YOLO模型对视频进行目标跟踪，并将结果保存到发送到kafka。

    Args:
        camera (str): Camera identifier string
        stop_event (threading.Event): Event to signal the function to stop
        device (str): 设备类型 ('cpu', 'cuda', 'cuda:0', 'cuda:1'等)
        pace (int): 帧间隔,几帧取1帧
    """
    cfg_file = f'{camera}.json'
    try:
        with open(cfg_file) as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"错误: 找不到配置文件 {cfg_file}")
        log_message(camera, f"错误: 找不到配置文件 {cfg_file}")
        return

    # 2. 打开视频文件
    cap = cv2.VideoCapture(config['source'])
    if not cap.isOpened():
        print(f"错误: 无法打开视频文件 {config['source']}")
        log_message(camera, f"错误: 无法打开视频文件 {config['source']}")
        return

    # 获取视频信息
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    print(f"视频信息: 总帧数={total_frames}, FPS={fps:.2f}")
    log_message(camera, f"视频信息: 总帧数={total_frames}, FPS={fps:.2f}")

    global yolo_model
    
    print(f"开始预处理视频 for camera {camera}...")
    log_message(camera, f"开始预处理视频 for camera {camera}")

    timestamp = None
    redlight_queue = FixedFIFO(maxlen=int(fps/pace/2 + 1))
    
    timestamp_area_left = config['timestamp']['left']
    timestamp_area_bottom = config['timestamp']['bottom']
    redlight_x1, readlight_y1, redlight_x2, redlight_y2 = config['light']

    while True:
        ret, frame = cap.read()
        if not ret:
            print(f"视频处理完毕 或者 处理视频出错 {camera}")
            log_message(camera, f"视频处理完毕 或者 处理视频出错 {camera}")
            break
        timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, timestamp_area_left, timestamp_area_bottom)
        red_light_area = frame[readlight_y1: redlight_y2, redlight_x1:redlight_x2]
        
        red_light = lit(red_light_area)
        redlight_queue.push(red_light)

        if  timestamp is not None and redlight_queue.is_full():
            break
    
    print(f"开始处理视频 for camera {camera}...")
    log_message(camera, f"开始处理视频 for camera {camera}")
    frame_num = 0
    interval = 1000 // fps * pace
    std_timestamp = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_frame_timestamp_obj = std_timestamp

    lines = []
    try:
        while True:
            # Check if stop event was triggered
            if stop_event and stop_event.is_set():
                print(f"停止信号接收到，结束处理视频 for camera {camera}")
                log_message(camera, f"停止信号接收到，结束处理视频 for camera {camera}")
                break
            
            ret, frame = cap.read()
            if not ret:
                print(f"视频处理完毕 for camera {camera}")
                log_message(camera, f"视频处理完毕 for camera {camera}")
                break
            
            frame_num += 1
            # if frame_num % pace != 0:             # 每隔pace帧处理一次,这个地方要详细考虑一下，当前测试视频并未到每秒25帧。
                # continue
            
            timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, timestamp_area_left, timestamp_area_bottom)
            # print(f"帧 {frame_num} 识别到的时间戳: {timestamp}")

            if timestamp is None :
                if last_frame_timestamp_obj == std_timestamp:
                    continue
                else:
                    timestamp_obj = last_frame_timestamp_obj + timedelta(milliseconds=interval)
                    timestamp = timestamp_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    
            last_frame_timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

            
            red_light_area = frame[readlight_y1: redlight_y2, redlight_x1:redlight_x2]
            red_light = lit(red_light_area)

            redlight_queue.push(red_light)

            #在红灯灭之前，会闪烁半秒时间，按照13帧每秒，则闪烁大约6帧，若pace为3即3帧取1帧，则闪烁2帧。
            # 因此，如果当前帧不是红灯，则检查上一帧和前两帧是否是红灯
            if not red_light: 
                if redlight_queue.get(-2) or redlight_queue.get(-3):
                    red_light = True
            

            # 4. 对当前帧进行跟踪
            results = yolo_model.track(source=frame, persist=True, verbose=False, conf=0.45)

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

                        # 同时保留到lines数组中（为了兼容旧代码，但可以移除）
                        # line = f"{frame_num}, {id}, {x1:.2f}, {y1:.2f}, {x2:.2f}, {y2:.2f}, {class_id}, {confidence:.2f}\n"

                        #发送到rabbitmq        
                        global producer
                        try:
                            # 检查producer连接是否可用
                            if (producer and 
                                hasattr(producer, 'channel') and 
                                producer.channel and 
                                not getattr(producer.channel, 'is_closed', False) and
                                hasattr(producer, 'connection') and
                                producer.connection and
                                not getattr(producer.connection, 'is_closed', False)):
                                producer.publish(camera, json.dumps(detection_data), ttl_ms=86400000) 
                            else:
                                print(f"RabbitMQ连接不可用，跳过发送数据 for camera {camera}")
                                log_message(camera, f"RabbitMQ连接不可用，跳过发送数据")
                        except Exception as e:
                            print(f"发送数据到RabbitMQ失败: {e}")
                            log_message(camera, f"发送数据到RabbitMQ失败: {e}")
                            # 如果是连接错误，记录错误但继续处理视频
                            # 这样可以确保视频处理完成，即使消息队列连接有问题
                            error_str = str(e).lower()
                            if 'connection' in error_str or 'channel' in error_str or 'stream' in error_str:
                                print(f"检测到RabbitMQ连接问题，跳过发送数据并继续处理视频 for camera {camera}")
                                log_message(camera, f"检测到RabbitMQ连接问题，跳过发送数据并继续处理视频")
                                # 根据规范，当检测到关键外部服务（如RabbitMQ）连接断开或通道关闭等不可用状态时，
                                # 不应继续尝试向其发送数据，应立即将此类错误识别为不可恢复错误，
                                # 并触发相关业务流程的停止事件
                                if stop_event and not stop_event.is_set():
                                    print(f"触发停止事件以优雅退出视频处理 for camera {camera}")
                                    log_message(camera, f"触发停止事件以优雅退出视频处理")
                                    stop_event.set()
                                    break
                        line = json.dumps(detection_data)
                        # print(line)
                        lines.append(line)

                # 可选：在控制台打印进度
            if frame_num % 30 == 0: # 每30帧打印一次
                progress_msg = f"已处理帧: {frame_num} / {total_frames}"
                print(progress_msg)
                log_message(camera, progress_msg)
            # last_frame_timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    finally:
        # 6. 释放资源
        cap.release()
        print(f"视频处理完成 for camera {camera}")
        log_message(camera, f"视频处理完成 for camera {camera}")
        
        # Mark as not running anymore and clean up the process entry
        if camera in active_processes:
            active_processes[camera]['running'] = False
            # Remove the entry from active_processes to allow restart
            del active_processes[camera]
    # 把lines保存到文件 
    with open(f'time_redlight_{camera}.txt', 'w') as f:
        f.writelines(lines)


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
                         command: str = Query(..., pattern="^(start|stop|list)$", description="Command: 'start', 'stop' or 'list'")):
    """
    Control endpoint to handle camera commands.
    
    Args:
        camera (str): Camera identifier string
        command (str): Command - either 'start', 'stop' or 'list'
    
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
            # If the camera exists in active_processes but is not running, 
            # it means the process has completed normally, so we can remove it and restart
            else:
                # Remove the old entry to allow restart
                del active_processes[camera]
        
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
    elif command == "list":
        print("Listing active processes")
        
        # Get list of currently running cameras
        running_cameras = []
        for cam, proc_info in active_processes.items():
            if proc_info['running']:
                running_cameras.append(cam)
        
        return {
            "command": command,
            "status": "success",
            "running_processes": len(running_cameras),
            "cameras": running_cameras,
            "message": f"Currently running {len(running_cameras)} video processing processes"
        }
    else:
        return {"error": "Invalid command. Use 'start', 'stop' or 'list'"}


@app.get("/monitor")
async def monitor_page():
    """Return the monitoring page HTML"""
    html_content = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>视频处理监控</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        .header {
            background-color: #4CAF50;
            color: white;
            padding: 20px;
            text-align: center;
        }
        .content {
            display: flex;
            flex-direction: column;
            height: 80vh;
        }
        .upper-part {
            flex: 1;
            padding: 20px;
            border-bottom: 1px solid #ddd;
            overflow-y: auto;
            min-height: 300px;
        }
        .lower-part {
            flex: 1;
            padding: 20px;
            background-color: #f9f9f9;
            overflow-y: auto;
            min-height: 300px;
        }
        .process-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px;
            margin: 10px 0;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: #fff;
        }
        .process-info {
            flex-grow: 1;
        }
        .process-name {
            font-weight: bold;
            font-size: 16px;
        }
        .process-status {
            margin-top: 5px;
            font-size: 14px;
            color: #666;
        }
        .process-controls {
            display: flex;
            gap: 10px;
        }
        .btn {
            padding: 8px 16px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
        }
        .btn-start {
            background-color: #4CAF50;
            color: white;
        }
        .btn-stop {
            background-color: #f44336;
            color: white;
        }
        .btn:hover {
            opacity: 0.8;
        }
        .log-container {
            height: 100%;
            overflow-y: auto;
            background-color: #000;
            color: #00ff00;
            padding: 10px;
            font-family: monospace;
            border-radius: 4px;
        }
        .log-entry {
            margin-bottom: 5px;
            white-space: nowrap;
        }
        .selected {
            border: 2px solid #4CAF50;
            background-color: #e8f5e8;
        }
        .no-selection {
            display: flex;
            align-items: center;
            justify-content: center;
            height: 100%;
            color: #999;
            font-style: italic;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>视频处理监控面板</h1>
        </div>
        <div class="content">
            <div class="upper-part" id="upperPart">
                <h2>视频处理程序列表</h2>
                <div id="processList">加载中...</div>
            </div>
            <div class="lower-part">
                <h2>日志输出</h2>
                <div id="logContainer" class="no-selection">
                    请选择一个视频处理程序以查看日志
                </div>
            </div>
        </div>
    </div>

    <script>
        let selectedCamera = null;
        let currentWebSocket = null;

        // 获取并显示进程列表
        async function loadProcessList() {
            const response = await fetch('/control?camera=dummy&command=list');
            const data = await response.json();
            
            const processListElement = document.getElementById('processList');
            processListElement.innerHTML = '';
            
            // Display all cameras (whether running or not)
            for (const [cameraId, processInfo] of Object.entries(window.activeProcesses || {})) {
                addProcessItem(processListElement, cameraId, processInfo.running);
            }
            
            // If no processes exist yet, show a message
            if (Object.keys(window.activeProcesses || {}).length === 0) {
                processListElement.innerHTML = '<p>暂无视频处理程序</p>';
            }
        }

        // 添加进程项到列表
        function addProcessItem(container, cameraId, isRunning) {
            const item = document.createElement('div');
            item.className = 'process-item';
            item.dataset.camera = cameraId;
            
            item.innerHTML = `
                <div class="process-info">
                    <div class="process-name">${cameraId}</div>
                    <div class="process-status">状态: ${isRunning ? '运行中' : '已停止'}</div>
                </div>
                <div class="process-controls">
                    <button class="btn btn-start" onclick="controlCamera('${cameraId}', 'start')">启动</button>
                    <button class="btn btn-stop" onclick="controlCamera('${cameraId}', 'stop')">停止</button>
                </div>
            `;
            
            item.addEventListener('click', () => selectProcess(item, cameraId));
            container.appendChild(item);
        }

        // 选择进程
        function selectProcess(element, cameraId) {
            // Remove selection from previously selected item
            document.querySelectorAll('.process-item').forEach(item => {
                item.classList.remove('selected');
            });
            
            // Add selection to clicked item
            element.classList.add('selected');
            selectedCamera = cameraId;
            
            // Update log container
            const logContainer = document.getElementById('logContainer');
            logContainer.className = 'log-container';
            logContainer.innerHTML = '';
            
            // Connect to WebSocket for this camera
            connectToWebSocket(cameraId);
        }

        // 控制相机
        async function controlCamera(cameraId, command) {
            const response = await fetch(`/control?camera=${encodeURIComponent(cameraId)}&command=${command}`);
            const data = await response.json();
            
            console.log(`Command ${command} for ${cameraId}:`, data);
            
            // Refresh the process list
            setTimeout(loadProcessList, 500);
            
            // If we stopped the currently selected camera, clear the logs
            if (cameraId === selectedCamera && command === 'stop') {
                const logContainer = document.getElementById('logContainer');
                logContainer.innerHTML = '';
            }
        }

        // 连接到WebSocket
        function connectToWebSocket(cameraId) {
            // Close existing connection if any
            if (currentWebSocket) {
                currentWebSocket.close();
            }
            
            // Create new WebSocket connection
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws/${cameraId}`;
            currentWebSocket = new WebSocket(wsUrl);
            
            currentWebSocket.onopen = function(event) {
                console.log('WebSocket connected for camera:', cameraId);
            };
            
            currentWebSocket.onmessage = function(event) {
                const logEntry = JSON.parse(event.data);
                displayLogEntry(logEntry);
            };
            
            currentWebSocket.onerror = function(error) {
                console.error('WebSocket error:', error);
            };
            
            currentWebSocket.onclose = function(event) {
                console.log('WebSocket disconnected for camera:', cameraId);
            };
        }

        // 显示日志条目
        function displayLogEntry(logEntry) {
            const logContainer = document.getElementById('logContainer');
            const logDiv = document.createElement('div');
            logDiv.className = 'log-entry';
            logDiv.textContent = `[${logEntry.timestamp}] ${logEntry.message}`;
            logContainer.appendChild(logDiv);
            
            // 自动滚动到底部
            logContainer.scrollTop = logContainer.scrollHeight;
        }

        // 初始化页面
        document.addEventListener('DOMContentLoaded', function() {
            // Load initial process list
            loadProcessList();
            
            // Refresh the list every 5 seconds
            setInterval(loadProcessList, 5000);
        });
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)


@app.websocket("/ws/{camera}")
async def websocket_endpoint(websocket: WebSocket, camera: str):
    """WebSocket endpoint for sending logs for a specific camera"""
    await websocket.accept()
    
    # Add this connection to the camera's connections list
    if camera not in websocket_connections:
        websocket_connections[camera] = []
    websocket_connections[camera].append(websocket)
    
    try:
        # Send initial logs if available
        if camera in process_logs:
            for log_entry in process_logs[camera]:
                await websocket.send_text(json.dumps(log_entry))
        
        # Keep the connection alive
        while True:
            # Just keep the connection open to receive messages
            data = await websocket.receive_text()
            # We don't expect to receive messages from client in this case
    except:
        # Remove the connection when client disconnects
        if camera in websocket_connections and websocket in websocket_connections[camera]:
            websocket_connections[camera].remove(websocket)


# --- Main program for backward compatibility ---
if __name__ == "__main__":

    global producer
    producer = Producer(amqp_url = 'amqp://admin:zhxk12345@192.168.1.142:5672/')

    if producer is None:
        print("无法创建数据源Producer实例！")
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