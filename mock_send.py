'''从文件中读出，模拟发送数据给redis
'''

import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0)

with open('time_redlight_330282000000010162.txt', 'r') as f:
    data = f.read()

data = data.replace('},{',"},\n{")
tracks = data.split()   

for i in tracks:
    print(i)
    json_data = json.loads(i)
    print(json_data)
    r.lpush('330282000000010162', json_data)
    print(json_data)


