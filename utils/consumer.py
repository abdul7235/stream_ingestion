import os
import json
import pika
import base64
import time
import cv2
import numpy as np
import wave
import threading
from queue_message_count import message_count
import time

last_message_time = time.time()
consecutive_zero_count = 0

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'
#QUEUE_NAME = '1234_stream_data'
 


def save_image_frame(uid, timestamp, frame_data):
    frame_array = np.frombuffer(base64.b64decode(frame_data), dtype=np.uint8)
    frame = cv2.imdecode(frame_array, flags=cv2.IMREAD_COLOR)
    cv2.imwrite(f'data/{uid}_{timestamp}.jpg', frame)

def save_audio(uid, timestamp, audio_data):
    audio_path = f'data/{uid}_{timestamp}.wav'
    with wave.open(audio_path, 'wb') as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(44100)
        # Convert audio_data to bytes before writing
        wav_file.writeframes(audio_data.encode())

'''def callback(ch, method, properties, body, stop_flag, connection):
    data = json.loads(body)
    uid = data['uid']
    timestamp = data['timestamp']
    frame_data = data['frame']
    audio_data = data['audio']

    save_image_frame(uid, timestamp, frame_data)
    save_audio(uid, timestamp, audio_data)

    print(f"Processed message: {uid}_{timestamp}")

    if stop_flag[0] == True:
        ch.stop_consuming()
        #connection.close()'''


def callback(ch, method, properties, body):
    global last_message_time
    global consecutive_zero_count
    
    if body:
        last_message_time = time.time()
        consecutive_zero_count = 0
        
        data = json.loads(body)
        uid = data['uid']
        timestamp = data['timestamp']
        frame_data = data['frame']
        audio_data = data['audio']

        save_image_frame(uid, timestamp, frame_data)
        save_audio(uid, timestamp, audio_data)

        print(f"Processed message: {uid}_{timestamp}")
    else:
        current_time = time.time()
        if current_time - last_message_time >= 60:
            consecutive_zero_count += 1
            if consecutive_zero_count >= 60:
                ch.stop_consuming()
                exit()
        else:
            consecutive_zero_count = 0



def subscriber(uid):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    stop_flag= [False]
    QUEUE_NAME = f"{uid}_stream_data"
    channel.queue_declare(queue=QUEUE_NAME)
    #thread_message_count = threading.Thread(target=message_count, args=(stop_flag, QUEUE_NAME))
    #thread_message_count.start()
    #callback_wrapper = lambda ch, method, properties, body: callback(ch, method, properties, body, stop_flag,connection)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()


subscriber('123456')