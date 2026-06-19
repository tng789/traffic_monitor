import json
from datetime import datetime
import asyncio
from typing import Dict, List
from fastapi import WebSocket
import threading

import logging

from app_log import get_logger

logger = get_logger(__name__)

# Global variables for logs and WebSocket connections
process_logs: Dict[str, List[Dict]] = {}  # Dictionary to store logs for each camera
websocket_connections: Dict[str, List[WebSocket]] = {}  # Dictionary to store WebSocket connections for each camera
active_processes: Dict[str, dict] = {}  # Dictionary to keep track of active video tracking processes


def log_message(camera, message, level=logging.INFO):
    """Log a message for a specific camera (file log + Web UI buffer)."""
    logger.log(level, "[%s] %s", camera, message)

    global process_logs
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
            except Exception as e:
                logger.warning(
                    "WebSocket 推送失败，移除连接 camera=%s: %s", camera, e, exc_info=True
                )
                if connection in websocket_connections[camera]:
                    websocket_connections[camera].remove(connection)


async def send_log_to_websocket(websocket, log_entry):
    """Send a log entry to a WebSocket connection"""
    try:
        await websocket.send_text(json.dumps(log_entry))
    except Exception as e:
        logger.warning("无法发送消息到 WebSocket: %s", e, exc_info=True)