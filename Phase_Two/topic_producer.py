import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Topic exchange declare karo
channel.exchange_declare(
    exchange='orders_topic',
    exchange_type='topic',
    durable=True
)

# Routing key aur message command line se lo
routing_key = sys.argv[1] if len(sys.argv) > 1 else 'order.created.basic'
message = ' '.join(sys.argv[2:]) or 'Order placed'

channel.basic_publish(
    exchange='orders_topic',
    routing_key=routing_key,     # e.g. "order.created.premium"
    body=message,
    properties=pika.BasicProperties(delivery_mode=2)
)

print(f" [x] Sent '{routing_key}': '{message}'")
connection.close()


