import pika

# 1. Connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# 2. Fanout exchange declare karo
channel.exchange_declare(
    exchange='news',
    exchange_type='fanout'      # ⭐ fanout
)

# 3. Message bhejo
message = "Breaking: Stock market crashed!"

channel.basic_publish(
    exchange='news',
    routing_key='',             # fanout mein ignore hoti hai
    body=message
)

print(f" [x] Broadcast: '{message}'")
connection.close()



