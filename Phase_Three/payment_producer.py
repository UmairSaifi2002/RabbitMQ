import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Publisher confirms enable
channel.confirm_delivery()

# Durable exchange
channel.exchange_declare(
    exchange='payments',
    exchange_type='direct',
    durable=True
)

# DLX setup
channel.exchange_declare(exchange='payments_dlx', exchange_type='direct', durable=True)
channel.queue_declare(queue='payments_dlq', durable=True)
channel.queue_bind(exchange='payments_dlx', queue='payments_dlq', routing_key='failed')

# Main queue with DLX and TTL
channel.queue_declare(
    queue='payments_queue',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'payments_dlx',
        'x-dead-letter-routing-key': 'failed',
        'x-message-ttl': 3600000     # 1 hour
    }
)

channel.queue_bind(exchange='payments', queue='payments_queue', routing_key='payment')

# Publish payment
payment = {"id": 12345, "amount": 599.00, "user": 42}

try:
    channel.basic_publish(
        exchange='payments',
        routing_key='payment',
        body=json.dumps(payment),
        properties=pika.BasicProperties(
            delivery_mode=2,        # Persistent
            priority=5              # Medium priority
        )
    )
    print(f"✅ Payment confirmed: {payment}")
except Exception as e:
    print(f"❌ Failed: {e}")

connection.close()



