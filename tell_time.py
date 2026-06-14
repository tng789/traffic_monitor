import cv2
import pytesseract
from datetime import datetime
import time
# import easyocr

# 配置 Tesseract 路径 (Windows下需要，Linux/Mac通常不需要)
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def fix(timetext:str)->str:
    # timetext的正确样例是这样的 2026-04-27 07:01:47.642
    # 不是这种形式的时间样式都需要纠正, 比如：20260427 07:01:47.132， 20260427 07:01:33..206， 
    # 像这种0267-04-27 07:38:13.215， 267-04-27 07:38:13.445 字符串中年份无法猜测，则属于不能修复，可返回None
    # 
    # 纠正之后还要再看看是否与样例的格式一致。如是， 返回符合样例的格式的timetext，如果不是，返回None

    import re
    from datetime import datetime
    
    # Check if the input already matches the correct format (with milliseconds)
    correct_format_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$'
    if re.match(correct_format_pattern, timetext):
        # Even if it matches the correct format, we still need to validate the year
        year_part = timetext.split('-')[0]
        if int(year_part) < 2020:
            return None
        return timetext
    
    # Check if the input matches the format without milliseconds (YYYY-MM-DD HH:MM:SS)
    no_ms_format_pattern = r'^(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})$'
    match_no_ms = re.match(no_ms_format_pattern, timetext)
    if match_no_ms:
        year, month, day, hour, minute, second = match_no_ms.groups()
        
        # Pad single digits with zero
        month = month.zfill(2)
        day = day.zfill(2)
        hour = hour.zfill(2)
        minute = minute.zfill(2)
        second = second.zfill(2)
        
        # Validate year range (should be >= 2020)
        year_int = int(year)
        if year_int < 2020:
            return None
            
        # Validate month and day ranges
        month_int = int(month)
        day_int = int(day)
        if month_int < 1 or month_int > 12 or day_int < 1 or day_int > 31:
            return None
        
        # Construct the formatted time string with .000
        formatted_time = f"{year}-{month}-{day} {hour}:{minute}:{second}.000"
        
        # Validate the constructed time string
        try:
            datetime.strptime(formatted_time, '%Y-%m-%d %H:%M:%S.%f')
            return formatted_time
        except ValueError:
            return None
    
    # First, clean up multiple dots in the millisecond part
    cleaned_text = re.sub(r'\.{2,}', '.', timetext)
    
    # Try to match various date-time formats with separators
    # Pattern for YYYY-M-D H:M:S.mmm, YYYY-MM-DD H:M:S.mmm, YYYY-M-DD H:M:S.mmm, etc.
    patterns = [
        r'(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})\.(\d+)',  # With milliseconds
        r'(\d{4})-(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})'          # Without milliseconds
    ]
    
    for pattern in patterns:
        match = re.search(pattern, cleaned_text)
        if match:
            groups = match.groups()
            if len(groups) == 7:  # With milliseconds
                year, month, day, hour, minute, second, millisecond = groups
                if len(millisecond) >= 3:
                    millisecond = millisecond[:3]
                else:
                    millisecond = millisecond.ljust(3, '0')
            else:  # Without milliseconds
                year, month, day, hour, minute, second = groups
                millisecond = "000"  # Always add default milliseconds when missing
            
            # Pad single digits with zero
            month = month.zfill(2)
            day = day.zfill(2)
            hour = hour.zfill(2)
            minute = minute.zfill(2)
            second = second.zfill(2)
            
            # Validate year range (should be >= 2020)
            year_int = int(year)
            if year_int < 2020:
                return None
                
            # Validate month and day ranges
            month_int = int(month)
            day_int = int(day)
            if month_int < 1 or month_int > 12 or day_int < 1 or day_int > 31:
                return None
            
            # Construct the formatted time string
            formatted_time = f"{year}-{month}-{day} {hour}:{minute}:{second}.{millisecond}"
            
            # Validate the constructed time string
            try:
                datetime.strptime(formatted_time, '%Y-%m-%d %H:%M:%S.%f')
                return formatted_time
            except ValueError:
                return None
    
    # If separator-based matching didn't work, try the pure digit extraction approach
    # Remove extra dots like in "20260427 07:01:33..206"
    timetext_with_cleaned_dots = re.sub(r'\.{2,}', '.', timetext)
    
    # Extract all digits
    digits = re.findall(r'\d', timetext_with_cleaned_dots)
    
    # If there are fewer than 14 digits (at least YYYYMMDDHHMMSS), format is invalid
    if len(digits) < 14:
        return None
    
    # Extract date/time components
    year = ''.join(digits[0:4])
    month = ''.join(digits[4:6])
    day = ''.join(digits[6:8])
    hour = ''.join(digits[8:10])
    minute = ''.join(digits[10:12])
    second = ''.join(digits[12:14])
    
    # Check if year is reasonable (should be >= 2020)
    try:
        year_int = int(year)
        if year_int < 2020:
            return None
    except ValueError:
        return None
    
    # Validate month and day ranges
    try:
        month_int = int(month)
        day_int = int(day)
        if month_int < 1 or month_int > 12 or day_int < 1 or day_int > 31:
            return None
    except ValueError:
        return None
    
    # Extract milliseconds if present
    millisecond = "000"  # Default to 000 if no milliseconds found
    if len(digits) > 14:  # More than 14 digits means we have milliseconds
        millisecond_digits = min(3, len(digits) - 14)  # Take up to 3 digits for milliseconds
        millisecond = ''.join(digits[14:14 + millisecond_digits])
        # Pad with zeros if necessary
        millisecond = millisecond.ljust(3, '0')
    
    # Construct the formatted time string
    formatted_time = f"{year}-{month}-{day} {hour}:{minute}:{second}.{millisecond}"
    
    # Validate the constructed time string
    try:
        # Parse the formatted time to ensure it's valid
        datetime.strptime(formatted_time, '%Y-%m-%d %H:%M:%S.%f')
        return formatted_time
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


if __name__ == '__main__':
    print(fix('20260427 07:01:47.132'))
    print(fix('2026-4-7 07:01:45..200'))       #月日必须两位数，尚未考虑，所以返回None，实际应不出现
    print(fix('20260427 07:01:33..206'))
    print(fix('0267-04-27 07:38:13.215'))
    print(fix('267-04-27 07:38:13.445'))