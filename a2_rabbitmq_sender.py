import os
# Kuyruğa veri gönderme fonksiyonu
import pika
import json
import os
queue_name = os.getenv("RABBITMQ_QUEUE", "interpol_notices_v4")
def get_channel():
    credentials = pika.PlainCredentials(
        username=os.getenv("RABBITMQ_USER", "guest"),
        password=os.getenv("RABBITMQ_PASS", "guest")
    )
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", 5672))
    ))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    return connection, channel

def send_to_queue(notice):
    connection, channel = get_channel()
    message = json.dumps(notice)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)  # mesaj kalıcı olsun
    )
    connection.close()

