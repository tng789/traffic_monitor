import cv2
import pytesseract
from datetime import datetime
# import easyocr

# 配置 Tesseract 路径 (Windows下需要，Linux/Mac通常不需要)
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def fix(timetext:str)->str:
    # timetext的正确样例是这样的 2026-04-27 07:01:47.642
    # 如果timetext中'-'的数量少于2个，或者':'的数量少于2个，或者'.'多于1个，则显然有错，需要纠正
    # 纠正的办法是在该有'-'、':'的位置上插入。多于的'.'则删除
    # 纠正之后还要再看看是否与样例的格式一致。如是， 返回符合样例的格式的timetext，如果不是，返回None

    import re
    
    # 检查timetext中'-'的数量，':'的数量，以及'.'的数量
    dash_count = timetext.count('-')
    colon_count = timetext.count(':')
    dot_count = timetext.count('.')
    
    # 提取所有的数字
    digits = re.findall(r'\d', timetext)
    
    # 如果数字不足14位（年4位+月2位+日2位+时2位+分2位+秒2位），则格式错误
    if len(digits) < 14:
        return None
    
    # 从数字中构建正确的格式
    year = ''.join(digits[0:4])
    month = ''.join(digits[4:6])
    day = ''.join(digits[6:8])
    hour = ''.join(digits[8:10])
    minute = ''.join(digits[10:12])
    second = ''.join(digits[12:14])
    
    # 检查是否还有毫秒部分
    millisecond = ""
    if len(digits) >= 17:  # 如果有至少3位毫秒数字
        millisecond = ''.join(digits[14:17])
    
    # 构建正确格式的时间字符串
    fixed_time = f"{year}-{month}-{day} {hour}:{minute}:{second}"
    if millisecond:
        fixed_time += f".{millisecond}"
    
    # 验证生成的时间字符串是否符合标准格式
    try:
        # 尝试解析生成的时间字符串，看是否符合标准格式
        if millisecond:
            parsed_time = datetime.strptime(fixed_time, '%Y-%m-%d %H:%M:%S.%f')
        else:
            parsed_time = datetime.strptime(fixed_time, '%Y-%m-%d %H:%M:%S')
        
        return fixed_time
    except ValueError:
        return None
def recognize_timestamp_cv2(img):

    #取图片的右上角1000*200大小的区域, 固定不变
    # 2. 定义 ROI (Region of Interest)
    roi  = img[0:200, 3000:]

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


def recognize_timestamp_easyocr(img, reader, widht=3000,height=200, model_storage_directory="./models"):
    # 初始化阅读器 (只加载英文模型，速度更快)
    # reader = easyocr.Reader(['en'], gpu=True,
                            #    model_storage_directory=model_storage_directory,
                            #    download_enabled=False)  # 禁止下载，使用本地模型
    # refion of interest
    roi = img[:height, widht:]

    # 识别
    result = reader.readtext(roi, detail=0, allowlist='0123456789:-.')

    # 合并结果

    result_str =  " ".join(result)

    # dt = datetime.strptime(result_str, '%Y-%m-%d %H:%M:%S.%f')
    # return dt
    return fix(result_str)


#if __name__ == '__main__':
#    print(fix('20260427 07:01:47.132'))
#    print(fix('2026-4-7 07:01:45..200'))
#    print(fix('20260427 07:01:33..206'))