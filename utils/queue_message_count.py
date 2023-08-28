import pika


RABBITMQ_HOST = 'localhost'


def message_count(stop_flag, queue_name):
    consecutive_false_count = 0
    false_list = []
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    queue_info = channel.queue_declare(queue=queue_name, passive=True)
    
    while True:
        message_count = queue_info.method.message_count
        
        if len(false_list) == 3000:
            stop_flag[0] = True  
            print(stop_flag[0])
            break

        if message_count == 0:
            consecutive_false_count += 1
            false_list.append(False)
        else:
            false_list = []

    #connection.close()
