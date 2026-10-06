import pika
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    print(f" [x] Processing: {body.decode()}")
    
    if random.random() < 0.5:
        # ❌ Fail — DLQ mein bhejo
        print(f" [✗] Failed → DLQ")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # ⭐ requeue=False
    else:
        # ✅ Success
        print(f" [✓] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='orders', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting...')
channel.start_consuming()



