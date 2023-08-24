import pika


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))  


channel = connection.channel()


queue_name = '123_audio'


channel.queue_declare(queue=queue_name)


queue_info = channel.queue_declare(queue=queue_name, passive=True)
message_count = queue_info.method.message_count


if message_count == 0:
    print(f"The queue '{queue_name}' is empty.")
else:
    print(f"The queue '{queue_name}' contains {message_count} messages.")


connection.close()
