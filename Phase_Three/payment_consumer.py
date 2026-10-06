import pika
import json
import time
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Prefetch — ek baar mein 1 message
channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    payment = json.loads(body)
    print(f" [x] Processing payment #{payment['id']} (${payment['amount']})")
    
    try:
        # Simulate processing
        time.sleep(1)
        
        # Random failure simulation
        if random.random() < 0.3:
            raise Exception("Payment gateway timeout")
        
        print(f" [✓] Payment #{payment['id']} success")
        ch.basic_ack(delivery_tag=method.delivery_tag)     # ✅ Ack
        
    except Exception as e:
        print(f" [✗] Payment #{payment['id']} failed: {e}")
        # ❌ Fail → requeue=False → DLQ mein jaayega
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='payments_queue', on_message_callback=callback, auto_ack=False)

print(' [*] Payment processor waiting...')
channel.start_consuming()



