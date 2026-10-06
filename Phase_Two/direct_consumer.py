import pika
import sys

# 1. Connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Exchange declare (same as producer — idempotent)
channel.exchange_declare(exchange='logs', exchange_type='direct', durable=True)

# 3. Apna temporary queue banao
#    exclusive=True → connection band hone pe queue delete ho jaayega
result = channel.queue_declare(queue='', exclusive=True) # queue='' -> it means that the server will create a random queue name for us
queue_name = result.method.queue

# 4. Command line se severity level lo
severities = sys.argv[1:] if len(sys.argv) > 1 else ['info']

# 5. Har severity ke liye binding banao
for severity in severities:
    channel.queue_bind(
        exchange='logs',
        queue=queue_name,
        routing_key=severity     # exactly match hoga
    )

print(f" [*] Waiting for logs (severities: {severities}). To exit press Ctrl+C")

# 6. Callback
def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}: {body.decode()}")

# 7. Consume
channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)
channel.start_consuming()


