import easyocr
import cv2

def recognize_timestamp_easyocr(image_path, model_storage_directory=None):
    # 初始化阅读器 (只加载英文模型，速度更快)
    # 如果提供了模型存储目录，则使用本地模型，否则允许在线下载
    if model_storage_directory:
        reader = easyocr.Reader(['en'], 
                               gpu=False,  # 如果有GPU设为True
                               model_storage_directory=model_storage_directory,
                               download_enabled=False)  # 禁止下载，使用本地模型
    else:
        reader = easyocr.Reader(['en'], gpu=False)  # 如果有GPU设为True

    # 读取图片
    img = cv2.imread(image_path)
    roi = img[:200, 2000:]
    h, w, _ = img.shape

    # 同样先裁剪 ROI，减少计算量
    # roi = img[0:int(h*0.2), int(w*0.55):w]

    # 识别
    result = reader.readtext(roi, detail=0, allowlist='0123456789:-.')

    # 合并结果
    return " ".join(result)

# 使用预加载模型的方式调用函数
# 需要先下载模型到本地目录，如：~/.EasyOCR/model 或自定义目录
result = recognize_timestamp_easyocr('sample.jpg', model_storage_directory='./models')

# 注意：第一次运行会下载模型，比较慢，之后就好了
# 如果想避免在线下载，可以使用如下方式：
# result = recognize_timestamp_easyocr('sample.jpg', model_storage_directory='/path/to/local/models')
# result = recognize_timestamp_easyocr('sample.jpg')
print(result)