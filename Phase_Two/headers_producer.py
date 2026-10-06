import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(
    exchange='reports_headers',
    exchange_type='headers'
)

# Message ke saath headers bhejo
channel.basic_publish(
    exchange='reports_headers',
    routing_key='',              # headers exchange mein ignore
    body="Monthly sales report",
    properties=pika.BasicProperties(
        headers={
            "format": "pdf",
            "type": "monthly",
            "priority": "high"
        }
    )
)

print(" [x] Sent monthly PDF report")
connection.close()