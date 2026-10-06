import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def callback(ch, method, properties, body):
    print(f" 💀 [DLQ] Failed message: {body.decode()}")
    # Yahan aap alert bhej sakte ho, ya manually review karo
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='orders_dlq', on_message_callback=callback, auto_ack=False)

print(' [*] Monitoring DLQ...')
channel.start_consuming()