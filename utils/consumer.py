import os
import json
import pika
import base64
import time
import cv2
import numpy as np
import wave

# RabbitMQ connection parameters
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'uid_stream_data'
 
# Maximum consecutive empty queue count before exiting
MAX_EMPTY_COUNT = 60

def save_image_frame(uid, timestamp, frame_data):
    frame_array = np.frombuffer(base64.b64decode(frame_data), dtype=np.uint8)
    frame = cv2.imdecode(frame_array, flags=cv2.IMREAD_COLOR)
    cv2.imwrite(f'{uid}_{timestamp}.jpg', frame)

def save_audio(uid, timestamp, audio_data):
    audio_path = f'{uid}_{timestamp}.wav'
    with wave.open(audio_path, 'wb') as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(44100)
        wav_file.writeframes(audio_data)

def callback(ch, method, properties, body):
    data = json.loads(body)
    uid = data['uid']
    timestamp = data['timestamp']
    frame_data = data['frame']
    audio_data = data['audio']

    save_image_frame(uid, timestamp, frame_data)
    save_audio(uid, timestamp, audio_data)

    print(f"Processed message: {uid}_{timestamp}")

    callback.count = getattr(callback, 'count', 0) + 1
    if callback.count >= MAX_EMPTY_COUNT:
        print("Exiting program due to empty queue.")
        ch.stop_consuming()

def subscriber(uid):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()
