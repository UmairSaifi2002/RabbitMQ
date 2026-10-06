import pika

# 1. Connection banao
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Queue declare karo (agar nahi hai toh bana do)
channel.queue_declare(queue='hello')

# 3. Default exchange pe message bhejo
channel.basic_publish(
    exchange='',              # '' = default exchange
    routing_key='hello',      # queue ka naam
    body='Hello RabbitMQ!'    # Our Message
)

print(" [x] Sent 'Hello RabbitMQ!'")
connection.close()


