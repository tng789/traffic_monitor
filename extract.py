from ultralytics import YOLO
import cv2
import sys
import redis
import json
from tell_time import recognize_timestamp_easyocr


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
    lower_red1 = cv2.np.array([0, 50, 50])
    upper_red1 = cv2.np.array([10, 255, 255])
    mask1 = cv2.inRange(hsv_roi, lower_red1, upper_red1)
    
    lower_red2 = cv2.np.array([170, 50, 50])
    upper_red2 = cv2.np.array([180, 255, 255])
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


def is_red_light_on_by_brightness(img, x1, y1, x2, y2, brightness_threshold=100):
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
    lower_red1 = cv2.np.array([0, 50, 50])
    upper_red1 = cv2.np.array([10, 255, 255])
    mask1 = cv2.inRange(hsv_roi, lower_red1, upper_red1)
    
    lower_red2 = cv2.np.array([170, 50, 50])
    upper_red2 = cv2.np.array([180, 255, 255])
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


def track_video(video_path,  model, config,  device='cpu', redis_host='localhost', redis_port=6379, redis_db=0):
    """
    使用YOLO模型对视频进行目标跟踪，并将结果保存到Redis。

    Args:
        video_path (str): 输入视频文件的路径。
        output_txt_path (str): 输出结果文本文件的路径（保留用于兼容性）。
        model_path (str): YOLO模型路径，默认为'best.pt'。
        device (str): 设备类型 ('cpu', 'cuda', 'cuda:0', 'cuda:1'等)
        redis_host (str): Redis服务器主机地址
        redis_port (int): Redis服务器端口
        redis_db (int): Redis数据库编号
    """
#    # 1. 加载YOLO模型
#    print(f"正在加载模型: {model_path}")
#    model = YOLO(model_path)
#    model.to(device)  # 将模型移动到指定设备
#    
#    print(f"模型将运行在: {device}")

    # 连接到Redis
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        # 测试连接
        r.ping()
        print(f"已连接到Redis服务器: {redis_host}:{redis_port}, DB: {redis_db}")
    except Exception as e:
        print(f"错误: 无法连接到Redis服务器: {e}")
        return

    # 2. 打开视频文件
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        print(f"错误: 无法打开视频文件 {video_path}")
        return

    # 获取视频信息
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    print(f"视频信息: 总帧数={total_frames}, FPS={fps:.2f}")

    # 清空之前可能存在的video相关的数据
    r.delete('video')
    
    lines = []
    frame_num = 0
    print("开始处理视频...")
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        frame_num += 1
        if frame_num % 10 != 0:
            continue
        
        timestamp = recognize_timestamp_easyocr(frame, config['timestamp'][0], config['timestamp'][1])

        red_light = is_red_light_on(frame, config['light'])

        # 4. 对当前帧进行跟踪
        # persist=True 确保目标ID在帧间保持一致
        results = model.track(source=frame, persist=True, verbose=False)

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
                        'timestamp': timestamp
                    }

                    # 将检测数据添加到Redis列表中
                    r.lpush('video', json.dumps(detection_data))
                    
                    # 同时保留到lines数组中（为了兼容旧代码，但可以移除）
                    line = f"{frame_num}, {id}, {x1:.2f}, {y1:.2f}, {x2:.2f}, {y2:.2f}, {class_id}, {confidence:.2f}\n"
                    lines.append(line)

            
            # 可选：在控制台打印进度
            # if frame_num % 30 == 0: # 每30帧打印一次
            #     print(f"已处理帧: {frame_num} / {total_frames}")

    # 6. 释放资源
    cap.release()

    # 保存原始文本格式到Redis，用于兼容性（可选）
    # 将lines作为一个整体字符串保存到Redis
    # full_text = ''.join(lines)
    # r.set(f'{video_path}:text', full_text)
    
    print(f"\n跟踪完成！结果已保存到Redis的 '{video_path}' 键中")
    print(f"总共保存了 {len(lines)} 条检测记录")

# --- 主程序 ---
if __name__ == "__main__":
    # 配置你的视频路径和输出文件路径
    input_video = sys.argv[1]           #"your_video.mp4"  # 替换为你的视频文件路径
    output_file = "results.txt"     # 替换为你想要的输出文件名
    
    with open(input_video, 'rt') as f:
        confs = json.load(f)

    print("正在装入检测模型")
    # 可以通过第三个参数指定设备，例如使用GPU: device='cuda'
    device = 'cuda' if len(sys.argv) < 3 else sys.argv[2]  # 默认使用GPU，如果提供了命令行参数则使用该参数指定的设备
    model = YOLO('best.pt')
    
    # 运行跟踪函数
    track_video(input_video, model, config = confs,  device=device)