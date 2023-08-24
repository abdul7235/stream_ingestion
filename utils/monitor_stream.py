import cv2

def monitor_rtmp_stream(cap, flag):
    consecutive_false_count = 0  # Counter for consecutive False occurrences
    false_list = []

    while True:
        if len(false_list) == 60:
            flag[0] = False
            print(flag[0])
            break
        ret, frame = cap.read()
        if ret:
            false_list = []
        
        if not ret:
            consecutive_false_count +=1
            false_list.append(False)

        print(flag[0])