import pika

# 1. Connection banao
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Queue declare karo (producer ne bana diya hoga, but safe hai)
channel.queue_declare(queue='hello')

# 3. Callback function — jab message aaye toh kya karo
def callback(ch, method, properties, body):
    print(f" [x] Received: {body.decode()}")

# 4. Queue pe consume karo
channel.basic_consume(
    queue='hello',
    on_message_callback=callback,
    auto_ack=True          # auto-acknowledge
)

print(' [*] Waiting for messages. To exit press Ctrl+C')
channel.start_consuming()

