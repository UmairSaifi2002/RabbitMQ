import pika
import sys

# 1. Connection banao
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Direct exchange declare karo
#    exchange="logs", type="direct"
channel.exchange_declare(
    exchange='logs',
    exchange_type='direct',
    durable=True           # server restart pe bhi rahe
)

# 3. Message bhejo — routing key ke saath
severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
message = ' '.join(sys.argv[2:]) or 'Hello World!'

channel.basic_publish(
    exchange='logs',
    routing_key=severity,      # "error", "warning", ya "info"
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2         # 2 = message persistent rahe
    )
)

print(f" [x] Sent '{severity}': '{message}'")
connection.close()


