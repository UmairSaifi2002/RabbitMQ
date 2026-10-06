import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Durable exchange
channel.exchange_declare(
    exchange='critical',
    exchange_type='direct',
    durable=True
)

# Durable queue
channel.queue_declare(queue='critical_queue', durable=True)

# Bind
channel.queue_bind(
    exchange='critical',
    queue='critical_queue',
    routing_key='critical'
)

# Persistent message
channel.basic_publish(
    exchange='critical',
    routing_key='critical',
    body='Payment #12345',
    properties=pika.BasicProperties(
        delivery_mode=2      # ⭐ Persistent
    )
)

print(" [x] Sent persistent message")
connection.close()