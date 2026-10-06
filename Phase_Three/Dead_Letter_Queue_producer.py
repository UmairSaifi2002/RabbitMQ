import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 1. Dead Letter Exchange (DLX) banao
channel.exchange_declare(
    exchange='dlx_exchange',
    exchange_type='direct',
    durable=True
)

# 2. Dead Letter Queue banao
channel.queue_declare(queue='orders_dlq', durable=True)

# 3. DLX ko DLQ se bind karo
channel.queue_bind(
    exchange='dlx_exchange',
    queue='orders_dlq',
    routing_key='failed'
)

# 4. Main queue banao — DLX configure karo
channel.queue_declare(
    queue='orders',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'dlx_exchange',      # DLX
        'x-dead-letter-routing-key': 'failed'          # DLQ routing key
    }
)

print("✅ Main queue + DLQ ready")
connection.close()