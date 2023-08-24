import pika
import os
import base64
import cv2


rabbitmq_host = 'localhost'
queue_name = '123_frames'


def save_image_to_disk(image_data, image_count):
    image_data = base64.b64decode(image_data)
    image_filename = f"frames/image_{image_count}.jpg"
    
    with open(image_filename, 'wb') as image_file:
        image_file.write(image_data)
    
    print(f"Image {image_count} saved to {image_filename}")


def callback(ch, method, properties, body):
    global image_count
    save_image_to_disk(body, image_count)
    image_count += 1


if not os.path.exists('frames'):
    os.mkdir('frames')


connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
channel = connection.channel()


channel.queue_declare(queue=queue_name)


image_count = 0


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)


print("Waiting for messages. To exit press CTRL+C")
channel.start_consuming()
