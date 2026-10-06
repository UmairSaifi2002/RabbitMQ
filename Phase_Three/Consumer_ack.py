import pika
import time
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks', durable=True)

def callback(ch, method, properties, body):
    task = body.decode()
    print(f" [x] Received: {task}")
    
    try:
        # Simulate work
        time.sleep(1)
        
        # 20% chance failure
        if random.random() < 0.2:
            raise Exception("Processing failed")
        
        print(f" [✓] Done: {task}")
        ch.basic_ack(delivery_tag=method.delivery_tag)   # ✅ Success
        
    except Exception as e:
        print(f" [✗] Failed: {task} — {e}")
        # Requeue → doosra consumer try karega
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

channel.basic_qos(prefetch_count=1)   # Ek baar mein 1 message (baad mein samjhenge)
channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting for tasks...')
channel.start_consuming()



