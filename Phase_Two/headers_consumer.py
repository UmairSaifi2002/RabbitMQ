import pika
import sys

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='reports_headers', exchange_type='headers')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

# Match mode: "all" (AND) ya "any" (OR)
match_mode = sys.argv[1] if len(sys.argv) > 1 else 'all'

# Binding headers
binding_headers = {
    "x-match": match_mode,
    "format": "pdf",
    "type": "monthly"
}

channel.queue_bind(
    exchange='reports_headers',
    queue=queue_name,
    arguments=binding_headers
)

print(f" [*] Waiting (match={match_mode}, filters={binding_headers})")

def callback(ch, method, properties, body):
    headers = properties.headers
    print(f" [x] Received: {body.decode()} | Headers: {headers}")

channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)
channel.start_consuming()