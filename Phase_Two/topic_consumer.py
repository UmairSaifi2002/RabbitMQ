import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='orders_topic', exchange_type='topic', durable=True)

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# Command line se binding patterns lo
binding_keys = sys.argv[1:] if len(sys.argv) > 1 else ['#']

for binding_key in binding_keys:
    channel.queue_bind(
        exchange='orders_topic',
        queue=queue_name,
        routing_key=binding_key     # wildcard patterns allowed
    )

print(f" [*] Waiting for patterns: {binding_keys}")

def callback(ch, method, properties, body):
    print(f" [x] Pattern matched: {method.routing_key} → {body.decode()}")

channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)
channel.start_consuming()


