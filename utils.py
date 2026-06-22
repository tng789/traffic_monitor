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

# Global RabbitMQ producer for log publishing (will be initialized later)
log_producer = None

# Store the main event loop for later use
_main_loop = None


def set_log_producer(producer):
    """Set the RabbitMQ producer for log publishing"""
    global log_producer
    log_producer = producer


def set_main_event_loop(loop):
    """Set the main event loop for use in other threads"""
    global _main_loop
    _main_loop = loop


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
        for connection in websocket_connections[camera][:]:  # Create a copy of the list to avoid modification during iteration
            try:
                # We're in a different thread, so we need to use run_coroutine_threadsafe
                # Use the stored main loop
                log_entry = {
                    'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'message': message
                }
                
                if _main_loop is not None:
                    # Use run_coroutine_threadsafe to safely send the message from this thread to the main event loop
                    future = asyncio.run_coroutine_threadsafe(
                        send_log_to_websocket(connection, log_entry),
                        _main_loop
                    )
                    
                    # Don't wait for the future to avoid blocking the calling thread
                else:
                    logger.warning(
                        "Main event loop not set, cannot send WebSocket message for camera=%s", camera
                    )
            except Exception as e:
                logger.warning(
                    "WebSocket 推送失败，移除连接 camera=%s: %s", camera, e, exc_info=True
                )
                if connection in websocket_connections[camera]:
                    websocket_connections[camera].remove(connection)
    
    # If RabbitMQ producer is available, also publish the log message for cross-process access
    if log_producer:
        try:
            log_entry = {
                'camera': camera,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'message': message,
                'level': level
            }
            # Publish to RabbitMQ using camera-specific routing key
            log_producer.publish(f"log.{camera}", log_entry)
        except Exception as e:
            logger.warning("Failed to publish log to RabbitMQ: %s", e, exc_info=True)


async def send_log_to_websocket(websocket, log_entry):
    """Send a log entry to a WebSocket connection"""
    try:
        await websocket.send_text(json.dumps(log_entry))
    except Exception as e:
        logger.warning("无法发送消息到 WebSocket: %s", e, exc_info=True)