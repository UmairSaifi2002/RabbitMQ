import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def callback(ch, method, properties, body):
    payment = json.loads(body)
    print(f" 💀 [DLQ] Payment #{payment['id']} needs manual review")
    # Yahan alert bhej sakte ho, dashboard update kar sakte ho
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='payments_dlq', on_message_callback=callback, auto_ack=False)

print(' [*] DLQ monitor running...')
channel.start_consuming()


