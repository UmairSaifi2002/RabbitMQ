import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# RPC request queue
channel.queue_declare(queue='rpc_queue', durable=False)

def on_request(ch, method, props, body):
    """Request handle karo aur reply bhejo."""
    n = int(body)
    print(f" [.] Processing: fib({n})")
    
    # Simulate work
    time.sleep(2)
    response = fib(n)
    
    # Reply bhejo — same correlation_id ke saath
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,             # callback queue
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id  # ⭐ match karne ke liye
        ),
        body=str(response)
    )
    
    # Original request ko ack karo
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(f" [✓] Sent fib({n}) = {response}")


def fib(n):
    if n < 2:
        return n
    return fib(n - 1) + fib(n - 2)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='rpc_queue', on_message_callback=on_request)

print(" [*] RPC Server waiting for requests...")
channel.start_consuming()