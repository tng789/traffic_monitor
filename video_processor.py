from ultralytics import YOLO
import cv2
import numpy as np
import json
from tell_time import recognize_timestamp_easyocr
from datetime import datetime, timedelta
import easyocr
import logging

from utils import log_message, active_processes
from app_log import get_logger

from fixed_fifo import FixedFIFO
from rmq import Producer
from track_processing import track_processor

logger = get_logger(__name__)

# Global variables to store pre-loaded models
yolo_model = None
easyocr_reader = None
producer = None


def lit(img):
    '''tell that if the light at the specific area(img) are lit
    '''
    #img, opened by cv2.imread
    frame = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, bw = cv2.threshold(frame,127,255,cv2.THRESH_BINARY)

    height, width, _ = img.shape
    whites = np.count_nonzero(bw==255)
    if whites * 4 > height * width:
        return True
    else:
        return False


def load_models():
    """Function to pre-load both YOLO and EasyOCR models"""
    global yolo_model, easyocr_reader

    logger.info("正在装入 YOLO 检测模型: ./models/best_v8s.pt")
    yolo_model = YOLO('./models/best_v8s.pt')

    logger.info("正在装入 EasyOCR 模型 (gpu=True)")
    easyocr_reader = easyocr.Reader(['en'], gpu=True,
                               model_storage_directory="./models",
                               download_enabled=False)
    logger.info("YOLO 与 EasyOCR 模型装载完成")


def track_video(camera, stop_event=None, device='cuda', pace = 3):
    """
    使用YOLO模型对视频进行目标跟踪，并将结果发送到 RabbitMQ。

    Args:
        camera (str): Camera identifier string
        stop_event (threading.Event): Event to signal the function to stop
        device (str): 设备类型 ('cpu', 'cuda', 'cuda:0', 'cuda:1'等)
        pace (int): 帧间隔,几帧取1帧
    """
    logger.info("开始 track_video camera=%s device=%s pace=%s", camera, device, pace)

    cfg_file = f'{camera}.json'
    try:
        with open(cfg_file) as f:
            config = json.load(f)
    except FileNotFoundError:
        msg = f"找不到配置文件 {cfg_file}"
        logger.error(msg)
        log_message(camera, msg, level=logging.ERROR)
        return
    except json.JSONDecodeError as e:
        msg = f"配置文件 JSON 解析失败 {cfg_file}: {e}"
        logger.exception(msg)
        log_message(camera, msg, level=logging.ERROR)
        return

    cap = cv2.VideoCapture(config['source'])
    if not cap.isOpened():
        msg = f"无法打开视频源 {config['source']}"
        logger.error(msg)
        log_message(camera, msg, level=logging.ERROR)
        return

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    logger.info("camera=%s 视频信息: 总帧数=%s FPS=%.2f source=%s", camera, total_frames, fps, config['source'])
    log_message(camera, f"视频信息: 总帧数={total_frames}, FPS={fps:.2f}")

    global yolo_model

    logger.info("camera=%s 开始预处理（同步时间戳与红灯状态）", camera)
    log_message(camera, f"开始预处理视频 for camera {camera}")

    timestamp = None
    redlight_queue = FixedFIFO(maxlen=int(fps/pace/2 + 1))

    timestamp_area_left = config['timestamp']['left']
    timestamp_area_bottom = config['timestamp']['bottom']
    redlight_x1, readlight_y1, redlight_x2, redlight_y2 = config['light']

    while True:
        ret, frame = cap.read()
        if not ret:
            msg = f"预处理阶段读取视频结束或出错 camera={camera}"
            logger.warning(msg)
            log_message(camera, msg, level=logging.WARNING)
            break
        timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, timestamp_area_left, timestamp_area_bottom)
        red_light_area = frame[readlight_y1: redlight_y2, redlight_x1:redlight_x2]

        red_light = lit(red_light_area)
        redlight_queue.push(red_light)

        if timestamp is not None and redlight_queue.is_full():
            logger.info("camera=%s 预处理完成，初始时间戳=%s", camera, timestamp)
            break

    logger.info("camera=%s 开始主循环处理", camera)
    log_message(camera, f"开始处理视频 for camera {camera}")
    
    # Create a track_processor instance to handle detection data directly
    processor = track_processor(camera_id=camera)
    
    frame_num = 0
    interval = 1000 // fps * pace
    std_timestamp = datetime.strptime("2020-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
    last_frame_timestamp_obj = std_timestamp

    lines = []
    try:
        while True:
            if stop_event and stop_event.is_set():
                msg = f"收到停止信号，结束处理 camera={camera}"
                logger.info(msg)
                log_message(camera, msg)
                break

            ret, frame = cap.read()
            if not ret:
                msg = f"视频处理完毕 camera={camera}"
                logger.info(msg)
                log_message(camera, msg)
                break

            frame_num += 1

            timestamp = recognize_timestamp_easyocr(frame, easyocr_reader, timestamp_area_left, timestamp_area_bottom)

            if timestamp is None:
                if last_frame_timestamp_obj == std_timestamp:
                    continue
                else:
                    timestamp_obj = last_frame_timestamp_obj + timedelta(milliseconds=interval)
                    timestamp = timestamp_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            last_frame_timestamp_obj = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

            red_light_area = frame[readlight_y1: redlight_y2, redlight_x1:redlight_x2]
            red_light = lit(red_light_area)

            redlight_queue.push(red_light)

            if not red_light:
                if redlight_queue.get(-2) or redlight_queue.get(-3):
                    red_light = True

            try:
                results = yolo_model.track(source=frame, persist=True, verbose=False, conf=0.45)
            except Exception:
                logger.exception("camera=%s frame=%s YOLO 跟踪失败", camera, frame_num)
                continue

            for result in results:
                if result.boxes is not None and result.boxes.id is not None:
                    boxes = result.boxes
                    xyxy = boxes.xyxy.cpu().numpy()
                    conf = boxes.conf.cpu().numpy()
                    cls = boxes.cls.cpu().numpy()
                    track_id = boxes.id.cpu().numpy()

                    for i in range(len(xyxy)):
                        x1, y1, x2, y2 = xyxy[i]
                        confidence = conf[i]
                        class_id = int(cls[i])
                        id = int(track_id[i])

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

                        # Process the detection data directly with the track_processor
                        # This will handle violations and send logs to the web UI
                        try:
                            processor.process_message(detection_data)
                        except Exception as e:
                            logger.exception("camera=%s frame=%s 处理检测数据失败", camera, frame_num)
                            log_message(camera, f"处理检测数据失败: {e}", level=logging.ERROR)

                        global producer
                        try:
                            if (producer and
                                hasattr(producer, 'channel') and
                                producer.channel and
                                not getattr(producer.channel, 'is_closed', False) and
                                hasattr(producer, 'connection') and
                                producer.connection and
                                not getattr(producer.connection, 'is_closed', False)):
                                producer.publish(camera, detection_data, ttl_ms=86400000)
                            else:
                                msg = "RabbitMQ 连接不可用，跳过发送数据"
                                logger.warning("camera=%s frame=%s %s", camera, frame_num, msg)
                                log_message(camera, msg, level=logging.WARNING)
                        except Exception as e:
                            logger.exception("camera=%s frame=%s 发送数据到 RabbitMQ 失败", camera, frame_num)
                            log_message(camera, f"发送数据到 RabbitMQ 失败: {e}", level=logging.ERROR)
                            error_str = str(e).lower()
                            if 'connection' in error_str or 'channel' in error_str or 'stream' in error_str:
                                msg = "检测到 RabbitMQ 连接问题，触发停止事件"
                                logger.error("camera=%s %s", camera, msg)
                                log_message(camera, msg, level=logging.ERROR)
                                if stop_event and not stop_event.is_set():
                                    stop_event.set()
                                    break

            if frame_num % 30 == 0:
                progress_msg = f"已处理帧: {frame_num} / {total_frames}"
                logger.info("camera=%s %s", camera, progress_msg)
                log_message(camera, progress_msg)

    finally:
        cap.release()
        msg = f"视频处理完成 camera={camera}，共处理 {frame_num} 帧"
        logger.info(msg)
        log_message(camera, f"视频处理完成 for camera {camera}")

        try:
            if camera in active_processes:
                active_processes[camera]['running'] = False
                del active_processes[camera]
        except Exception:
            logger.warning("清理 active_processes 失败 camera=%s", camera, exc_info=True)

    output_file = f'time_redlight_{camera}.txt'
    try:
        with open(output_file, 'w') as f:
            f.writelines(lines)
        logger.info("camera=%s 检测数据已保存到 %s，共 %s 条", camera, output_file, len(lines))
    except OSError:
        logger.exception("camera=%s 保存文件失败 %s", camera, output_file)