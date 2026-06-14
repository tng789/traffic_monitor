'''
ocrd读时间戳会出现null?
测试一下
'''
import easyocr
from pathlib import Path
import cv2

from tell_time import recognize_timestamp_easyocr

ocr_reader = easyocr.Reader(['en'], gpu=True,
                               model_storage_directory="./models",
                               download_enabled=False)  # 禁止下载，使用本地模型recognize_timestamp_easyocr(Path("./models"))

if __name__ == "__main__":
    #遍历指定文件夹下的所有图片文件
    for file in Path("/mnt/d/workspace/tmp/nb_nulls").glob("*.jpg"):
        # print(file.name)
        jpg = Path("/mnt/d/workspace/tmp/nb_nulls") / file.name
        # print(jpg)
        img = cv2.imread(str(jpg))
        print(recognize_timestamp_easyocr(img, ocr_reader))

    