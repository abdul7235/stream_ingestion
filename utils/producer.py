from utils.clip_duration import check_video_duration
import cv2
import os
import subprocess
import base64
import pika
import threading
from utils.monitor_stream import monitor_rtmp_stream


def publisher(rtmp_link, uid):
    #connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq_container'))
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    print(f"Hello {uid}")
    vid_dir = f"{uid}_chunks"
    aud_dir = f"{uid}_audios"

    queue_set = set()
    flag = [True]
    #channel.queue_declare(queue=f"{uid}_frames")
    #channel.queue_declare(queue=f"{uid}_audio")
    channel.queue_declare(queue=f"{uid}_stream_data")

    if not os.path.exists(vid_dir):
        os.makedirs(vid_dir)

    if not os.path.exists(aud_dir):
        os.makedirs(aud_dir)

    initial_cap = cv2.VideoCapture(rtmp_link)


    command = [
        'ffmpeg', '-i', rtmp_link, '-map', '0:a', '-map', '0:v', '-c:a', 'copy', '-c:v', 'libx264',
        '-preset', 'veryfast', '-g', '10', '-reset_timestamps', '1', '-segment_time', '1',
        '-f', 'segment', '-segment_format', 'mp4', '-strftime', '1',
        f'{vid_dir}/output_%Y-%m-%d_%H-%M-%S.mp4'
    ]
    process_main = subprocess.Popen(command, shell =False)
    stream_monitor_thread = threading.Thread(target=monitor_rtmp_stream, args=(initial_cap, flag))
    stream_monitor_thread.start()

    while flag[0]:
        stream_data ={}
        stream_data['uid'] = uid
        file_list = os.listdir(vid_dir)
        for filename in file_list:
            if (filename not in queue_set):

                if (check_video_duration(vid_dir, filename) >= 1.0):

                    queue_set.add(filename)
                    stream_data['timestamp'] = filenanme
                    cc =0
                    cap = cv2.VideoCapture(f"{vid_dir}/{filename}")

                    
                    while True:
                        ret, frame = cap.read()
                        if ret:
                            if cc % 40 == 0:
                                _, buffer = cv2.imencode('.jpg', frame)  
                                frame_base64 = base64.b64encode(buffer).decode("utf-8")
                                #channel.basic_publish(exchange='', routing_key=f"{uid}_frames", body=frame_base64)
                                stream_data['frame'] = frame_base64
                        else:
                            cap.release()
                            break
                        cc+=1
                    wav_file_name = filename[:-4]+".wav"
                    
                    command = ["ffmpeg", "-i" ,f"{vid_dir}/{filename}","-acodec", "pcm_s16le" ,"-ac", "1", "-ar" ,"16000","-y", 
                    f"{aud_dir}/{wav_file_name}" ]
                    
                    process_audio = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    audio_base64 = base64.b64encode(open(f"{aud_dir}/{wav_file_name}", "rb").read())
                    #channel.basic_publish(exchange='', routing_key=f"{uid}_audio", body=audio_base64)
                    stream_data['audio'] = audio_base64
                    channel.basic_publish(exchange='', routing_key=f"{uid}_stream_data", body=stream_data)
                    if os.path.exists(f"{vid_dir}/{filename}"):
                        os.remove(f"{vid_dir}/{filename}") 
                    if os.path.exists(f"{aud_dir}/{wav_file_name}"):
                        os.remove(f"{aud_dir}/{wav_file_name}")

    stream_monitor_thread.join()
    process_main.terminate()
    print("process terminated")



"""fmpeg -i rtmp://3.29.154.183/live/abdul70 -map 0:a -map 0:v -c:a copy -c:v libx264 -preset veryfast -g 10 -reset_timestamps 1 -segment_time 1 -f segment -segment_format mp4 -vf "select=gte(n\,1)" -q:v 1 -metadata:s:v:0 creation_time="$(date +'%Y-%m-%dT%H:%M:%S%z')" -strftime 1 output_%Y-%m-%d_%H-%M-%S.mp4"""
