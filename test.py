'''
{
    "camera": "330282000000010162",
    "source": "330282000000010162.mp4",
    "camera address": "浒山慈百路与新江路南口",
    "video size": [4096,2160],
    "valid area": [[0,2160],[0,1200],[1100,410],[2820,490],[3760,1080],[3150,2160]],
    "runredlight": [[750,680],[1100,410],[2820,490],[3760,1080]],
    "wrongdirection": {"direction":"up", "area": [[2500,2160], [2600, 640],[3110,640],[3760,1080],[3150,2160]]},
    "light":[1786,44,1794,52],
    "timestamp":[3000,200]
}
'''

import cv2
import json
def draw_polygon(img, polygon, color=(0, 255, 0)):
    for i in range(len(polygon) - 1):
        cv2.line(img, polygon[i], polygon[i+1], color, 2)
    cv2.line(img, polygon[-1], polygon[0], color, 2)
    return img

with open('330282000000010162.json', 'r') as f:
    data = json.load(f)
print(data)

img = cv2.imread("frame2_000902.jpg")

img = draw_polygon(img, data['valid area'])
img = draw_polygon(img, data['wrongdirection']['area'], color=(255, 0, 0))
img = draw_polygon(img, data['runredlight'], color=(0, 0, 255))

r = [(1786, 44), (1794, 44), (1794, 52), (1786, 52)]
img = draw_polygon(img, r, color=(255, 255, 0))

cv2.line(img, (data['light'][0], data['light'][1]), (data['light'][2], data['light'][3]), (0, 255, 255), 2)


cv2.imwrite("test.png", img) 