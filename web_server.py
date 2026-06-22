from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import asyncio
from typing import Dict, List
from datetime import datetime
import json
import threading
import uvicorn
import video_processor
from video_processor import track_video, load_models
from utils import log_message, send_log_to_websocket, active_processes, process_logs, websocket_connections, set_log_producer, set_main_event_loop
from rmq import Producer, Consumer, start_process, stop_process, cleanup_queue_messages
from app_log import setup_logging, get_logger
import logging
import os

logger = get_logger(__name__)
app = FastAPI(
    title="Video Processing API", 
    description="API for video processing with YOLO and EasyOCR"
)


def load_camera_configs():
    """Load camera configurations from the cameras directory"""
    cameras_dir = "cameras"
    camera_configs = {}
    
    if not os.path.exists(cameras_dir):
        logger.warning(f"Cameras directory '{cameras_dir}' does not exist")
        return camera_configs
    
    for filename in os.listdir(cameras_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(cameras_dir, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    # Use the filename (without extension) as the camera ID
                    camera_id = os.path.splitext(filename)[0]
                    camera_configs[camera_id] = config
                    logger.info(f"Loaded camera config for {camera_id} from {filename}")
            except Exception as e:
                logger.error(f"Error loading camera config from {filename}: {e}")
    
    return camera_configs


@app.on_event("startup")
async def startup_event():
    # Set the main event loop for use in other threads
    set_main_event_loop(asyncio.get_running_loop())
    
    # Initialize the log producer in utils
    try:
        log_producer = Producer(amqp_url='amqp://admin:zhxk12345@192.168.1.142:5672/')
        set_log_producer(log_producer)
        logger.info("Log producer initialized")
    except Exception as e:
        logger.error(f"Failed to initialize log producer: {e}")


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
        logger.info("收到启动命令 camera=%s", camera)

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
        
        # Start the RabbitMQ consumer for this camera in a separate thread
        consumer_thread = threading.Thread(target=start_process, args=(camera,), daemon=True)
        consumer_thread.start()
        
        # Create a stop event for this camera
        stop_event = threading.Event()
        
        # Create a thread for video processing
        video_thread = threading.Thread(
            target=track_video, 
            args=(camera, stop_event)
        )
        
        # Store the thread and stop event for both video processing and RabbitMQ consumer
        active_processes[camera] = {
            'thread': video_thread,
            'stop_event': stop_event,
            'consumer_thread': consumer_thread,
            'running': True,
            'consumer_started': True
        }
        
        # Start the video processing thread
        video_thread.start()
        
        return {
            "camera": camera,
            "command": command,
            "status": "processing_started",
            "message": f"Started processing and RabbitMQ consumer for camera {camera}"
        }
    elif command == "stop":
        logger.info("收到停止命令 camera=%s", camera)
        
        # Check if the camera is currently being processed
        if camera not in active_processes or not active_processes[camera]['running']:
            return {
                "camera": camera,
                "command": command,
                "status": "not_running",
                "message": f"Camera {camera} is not currently being processed"
            }
        
        # Step 1: Stop the video processing thread first
        active_processes[camera]['stop_event'].set()
        logger.info("已发送停止信号给视频处理线程 camera=%s", camera)
        
        # Wait briefly for the video processing thread to finish
        if 'thread' in active_processes[camera]:
            active_processes[camera]['thread'].join(timeout=2)
        
        # Step 2: Clean up RabbitMQ queue messages for this camera
        try:
            cleanup_queue_messages(camera)
            logger.info("已清理 camera=%s 在 RabbitMQ 中的相关数据", camera)
        except Exception as e:
            logger.error("清理 camera=%s 的 RabbitMQ 数据失败: %s", camera, e)
        
        # Step 3: Stop the RabbitMQ consumer process
        try:
            stop_process(camera)
            logger.info("已停止 camera=%s 的RabbitMQ消费者", camera)
        except Exception as e:
            logger.error("停止 camera=%s 的RabbitMQ消费者失败: %s", camera, e)
        
        # Mark as not running anymore
        active_processes[camera]['running'] = False
        
        return {
            "camera": camera,
            "command": command,
            "status": "processing_stopped", 
            "message": f"Stop signal sent for camera {camera} (video processing and RabbitMQ consumer)"
        }
    elif command == "list":
        logger.info("收到 list 命令，查询运行中的相机")
        
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


@app.get("/cameras")
async def get_cameras():
    """Return the list of available cameras from the config files"""
    camera_configs = load_camera_configs()
    cameras_list = []
    for camera_id, config in camera_configs.items():
        cameras_list.append({
            "id": camera_id,
            "config": config
        })
    return {"cameras": cameras_list}


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
        .process-config {
            margin-top: 5px;
            font-size: 12px;
            color: #888;
            word-break: break-all;
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
        let cameraConfigs = {};

        // 获取摄像机配置
        async function loadCameraConfigs() {
            const response = await fetch('/cameras');
            const data = await response.json();
            
            // Store camera configs for later use
            data.cameras.forEach(camera => {
                cameraConfigs[camera.id] = camera.config;
            });
            
            return data.cameras;
        }

        // 获取并显示进程列表
        async function loadProcessList() {
            const response = await fetch('/control?camera=dummy&command=list');
            const processData = await response.json();
            
            // Get all cameras from config files
            const allCameras = await loadCameraConfigs();
            
            const processListElement = document.getElementById('processList');
            processListElement.innerHTML = '';
            
            // Display all configured cameras
            if (allCameras.length === 0) {
                processListElement.innerHTML = '<p>未找到任何摄像机配置文件</p>';
            } else {
                allCameras.forEach(camera => {
                    const isRunning = processData.cameras.includes(camera.id);
                    addProcessItem(processListElement, camera.id, isRunning, camera.config);
                });
            }
        }

        // 添加进程项到列表
        function addProcessItem(container, cameraId, isRunning, config) {
            const item = document.createElement('div');
            item.className = 'process-item';
            item.dataset.camera = cameraId;
            
            // Limit config display to first 100 chars
            const configPreview = JSON.stringify(config).substring(0, 100) + (JSON.stringify(config).length > 100 ? '...' : '');
            
            item.innerHTML = `
                <div class="process-info">
                    <div class="process-name">${cameraId}</div>
                    <div class="process-status">状态: ${isRunning ? '运行中' : '已停止'}</div>
                    <div class="process-config">配置: ${configPreview}</div>
                </div>
                <div class="process-controls">
                    <button class="btn btn-start" onclick="controlCamera('${cameraId}', 'start')">启动</button>
                    <button class="btn btn-stop" onclick="controlCamera('${cameraId}', 'stop')" ${!isRunning ? 'disabled' : ''}>停止</button>
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
                try {
                    const logEntry = JSON.parse(event.data);
                    displayLogEntry(logEntry);
                } catch (e) {
                    console.error('Error parsing WebSocket message:', e);
                }
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


def start_server(host="0.0.0.0", port=8000, auto_start_cameras: List[str] = None):
    """Start the FastAPI server and optionally auto-start specified cameras
    
    Args:
        host: Host address to bind the server
        port: Port number to bind the server
        auto_start_cameras: List of camera IDs to automatically start when server starts
    """
    # Create the main RabbitMQ producer
    main_producer = Producer(amqp_url='amqp://admin:zhxk12345@192.168.1.142:5672/')
    video_processor.producer = main_producer

    if video_processor.producer is None:
        logger.error("无法创建 RabbitMQ Producer 实例")
        return

    # Set the log producer in utils
    set_log_producer(main_producer)

    logger.info("正在启动服务并装载模型...")
    load_models()
    logger.info("模型装载完成")

    if auto_start_cameras:
        logger.info("自动启动摄像头: %s", auto_start_cameras)
        for camera in auto_start_cameras:
            logger.info("自动启动 camera=%s", camera)

            # Check if the camera is already being processed
            if camera in active_processes:
                if active_processes[camera]['running']:
                    logger.warning("camera=%s 已在运行，跳过自动启动", camera)
                    continue
                # If the camera exists in active_processes but is not running, 
                # it means the process has completed normally, so we can remove it and restart
                else:
                    # Remove the old entry to allow restart
                    del active_processes[camera]
            
            # Start the RabbitMQ consumer for this camera
            consumer_thread = threading.Thread(target=start_process, args=(camera,), daemon=True)
            consumer_thread.start()
            
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
                'consumer_thread': consumer_thread,
                'running': True,
                'consumer_started': True
            }
            
            # Start the video processing thread
            video_thread.start()

            logger.info("camera=%s 自动启动完成", camera)

    logger.info("FastAPI 服务启动: host=%s port=%s", host, port)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    setup_logging()
    start_server()