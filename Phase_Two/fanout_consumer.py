import pika
import sys

# 1. Connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Fanout exchange declare
channel.exchange_declare(exchange='news', exchange_type='fanout')

# 3. Apna exclusive queue banao
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# 4. Exchange se bind karo (routing key ki zaroorat nahi)
channel.queue_bind(exchange='news', queue=queue_name)

# 5. Consumer name identify karne ke liye
consumer_name = sys.argv[1] if len(sys.argv) > 1 else 'unknown'
print(f" [*] [{consumer_name}] Waiting for news...")

# 6. Callback
def callback(ch, method, properties, body):
    print(f" [{consumer_name}] Received: {body.decode()}")

# 7. Consume
channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)
channel.start_consuming()


