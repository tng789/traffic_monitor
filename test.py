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
''''x1': 2604.134765625, 'y1': 734.2152709960938, 'x2': 2727.447509765625, 'y2': 967.8073120117188,
'x1': 1419.281005859375, 'y1': 612.4291381835938, 'x2': 1531.0489501953125, 'y2': 810.5841674804688,
'''
import cv2
import json
def draw_polygon(img, polygon, color=(0, 255, 0)):
    for i in range(len(polygon) - 1):
        cv2.line(img, polygon[i], polygon[i+1], color, 2)
    cv2.line(img, polygon[-1], polygon[0], color, 2)
    return img

#with open('330282000000010162.json', 'r') as f:
#    data = json.load(f)
#print(data)
#
#img = cv2.imread("frame2_000902.jpg")
#
#img = draw_polygon(img, data['valid area'])
#img = draw_polygon(img, data['wrongdirection']['area'], color=(255, 0, 0))
#img = draw_polygon(img, data['runredlight'], color=(0, 0, 255))

img = cv2.imread("frame2_000008.jpg")
r = [(1786, 44), (1794, 44), (1794, 52), (1786, 52)]
# r = [(),(),(),(),()]


# img = draw_polygon(img, r, color=(255, 255, 0))

# cv2.line(img, (data['light'][0], data['light'][1]), (data['light'][2], data['light'][3]), (0, 255, 255), 2)
# cv2.line(img, (2604,734), (2727,967), (0, 255, 255), 2)
cv2.line(img, (1419,612), (1531,810), (0, 255, 255), 2)


cv2.imwrite("test3.png", img) 