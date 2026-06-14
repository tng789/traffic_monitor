import json
from datetime import datetime, timedelta
from tell_time import fix
with open('time_redlight_330282000000010162.txt',"rt") as f:
    result = f.read()
    

fixed = result.replace("}{","},\n{")
# with open("time_redlight_330282000000010162_modified.txt","wt") as f:
    # f.write(fixed)

lines = fixed.splitlines()

prev_timestamp = "2023-01-01 00:00:00.000"

lit = 0
not_lit = 0
for i in range(len(lines)):
    try:
        obj = json.loads(lines[i].strip(","))
        next_obj = json.loads(lines[i+1].strip(","))

        redlight = obj['red_light']
        next_light = next_obj['red_light']
        
        if redlight != next_light:
            print(f"{lit=}\t{not_lit=}")    
        if redlight:
            not_lit = 0
            lit += 1
        else:
            not_lit += 1
            lit = 0
        
            
        
#for i in range(len(lines)-1):
#    try:
#        # print(line.rstrip(","))
#        obj = json.loads(lines[i].strip(","))
#        next_obj = json.loads(lines[i+1].strip(","))
#
#        redlight = obj['red_light']
#        # if redlight:
#            # print(redlight)
#
#        next_redlight = next_obj['red_light']
#
#        if redlight != next_redlight:
#            print(f"Red light changed into {next_obj['red_light']} at frame_id:", next_obj['frame_id'])

#        timestamp = obj['timestamp']
#
#        if obj['timestamp'] is None:
#            if i == 0:
#                continue
#            else:
#                timestamp_obj = datetime.strptime(prev_timestamp, "%Y-%m-%d %H:%M:%S.%f") + timedelta(milliseconds=230)
#                timestamp = timestamp_obj.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
#        # else:
#            # timestamp = fix(timestamp)
#        print(timestamp)
#        prev_timestamp = timestamp
    except Exception as e:
        print(e)

        