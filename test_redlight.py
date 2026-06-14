import cv2
import numpy as np

def lightup(img):
    '''tell that if the light at the specific area(img) are lit
    '''
    #img, opened by cv2.imread
    frame = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, bw = cv2.threshold(frame,127,255,cv2.THRESH_BINARY)

    height, width, _ = img.shape
    whites = np.count_nonzero(bw==255)
    # print(f"{whites=} area={height*width}")
    if whites * 4 > height * width:
        return True
    else:
        return False

if __name__ == "__main__":

    cap = cv2.VideoCapture("10min.mp4")
    if not cap.isOpened():
        print("错误: 无法打开视频文件 ")
        exit()

    # 获取视频信息
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = cap.get(cv2.CAP_PROP_FPS)
    print(f"视频信息: 总帧数={total_frames}, FPS={fps:.2f}")

    frame_num = 0
    while True:
    # Check if stop event was triggered
        
        ret, frame = cap.read()
        if not ret:
            print("视频处理完毕 for source file")
            break
        
        frame_num += 1
        # if frame_num % 3 != 0:             # 每隔pace帧处理一次,这个地方要详细考虑一下，当前测试视频并未到每秒25帧。
            # continue
        
        light_area = frame[42:56, 1782:1800]
        
        lit = lightup(light_area)
        if lit:
            print(f"{frame_num=} light is on")
        
        cv2.imwrite(f"./nulls/{frame_num:06d}_{lit}.png", light_area)