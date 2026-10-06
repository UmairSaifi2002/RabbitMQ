import pika
import uuid

class RPCClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.channel = self.connection.channel()
        
        # Exclusive callback queue — sirf iss client ke liye
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        
        self.response = None
        self.corr_id = None
    
    def on_response(self, ch, method, props, body):
        """Jab reply aaye."""
        if self.corr_id == props.correlation_id:
            self.response = body.decode()
    
    def call(self, n):
        """RPC call karo aur result return karo."""
        self.response = None
        self.corr_id = str(uuid.uuid4())        # Unique ID
        
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,    # callback queue batao
                correlation_id=self.corr_id       # unique ID
            ),
            body=str(n)
        )
        
        # Reply ka wait karo
        while self.response is None:
            self.connection.process_data_events()
        
        return self.response


if __name__ == "__main__":
    client = RPCClient()
    
    for n in [5, 10, 15]:
        print(f" [x] Requesting fib({n})")
        response = client.call(n)
        print(f" [.] Got: {response}")