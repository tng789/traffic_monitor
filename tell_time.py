import cv2
import pytesseract
from datetime import datetime
import easyocr

# 配置 Tesseract 路径 (Windows下需要，Linux/Mac通常不需要)
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def recognize_timestamp_cv2(img):

    #取图片的右上角1000*200大小的区域, 固定不变
    # 2. 定义 ROI (Region of Interest)
    roi  = img[0:200, 2000:]

    # 3. 预处理
    # 转灰度
    gray = cv2.cvtColor(roi, cv2.COLOR_BGR2GRAY)
    # 二值化 (OTSU 自动阈值)
    # 时间戳是白字，背景是深色/半透明，反转一下变成黑字白底通常更好识别
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    
    # 可选：形态学操作去噪
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    binary = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)

    # 4. OCR 识别
    # 配置参数：--psm 7 表示将其视为单行文本，whitelist 只允许数字和标点
    config = r'--psm 7 -c tessedit_char_whitelist=0123456789:-. '
    text = pytesseract.image_to_string(binary, config=config)
    
    return text.strip()


def recognize_timestamp_easyocr(img,widht=2000,height=200, model_storage_directory="./models"):
    # 初始化阅读器 (只加载英文模型，速度更快)
    reader = easyocr.Reader(['en'], gpu=False,
                               model_storage_directory=model_storage_directory,
                               download_enabled=False)  # 禁止下载，使用本地模型
    # refion of interest
    roi = img[:height, widht:]

    # 识别
    result = reader.readtext(roi, detail=0, allowlist='0123456789:-.')

    # 合并结果

    result_str =  " ".join(result)

    # dt = datetime.strptime(result_str, '%Y-%m-%d %H:%M:%S.%f')
    # return dt
    return result_str