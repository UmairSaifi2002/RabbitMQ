# RabbitMQ — Phase 4: Advanced Topics

Aaiye, ab hum RabbitMQ ke **advanced concepts** pe jaate hain. Yeh woh topics hain jo **production-scale** systems mein zaroori hote hain — jaise RPC, clustering, high availability.

**Phase 4 mein hum yeh cover karenge:**

1. RPC (Remote Procedure Call) Pattern
2. Clustering — Multiple Nodes
3. High Availability — Mirrored & Quorum Queues
4. Federation — Multi-Datacenter
5. Shovel — Cross-Broker Message Movement
6. Load Balancing — Multiple Consumers
7. Best Practices — Production Checklist

---

## 📋 Pehle — Kitne Phases Bache Hain?

Aapne ab tak 4 phases cover kiye (including yeh wala). Ab **do aur phases** bache hain:

| Phase | Topic | Status |
|---|---|---|
| Phase 1 | Introduction & Core Concepts | ✅ Done |
| Phase 2 | Exchange Types | ✅ Done |
| Phase 3 | Reliability & Acknowledgments | ✅ Done |
| **Phase 4** | **Advanced Topics (RPC, Clustering, HA)** | **🔄 Yeh chal raha hai** |
| Phase 5 | Management, Monitoring & Operations | ⏳ Pending |
| Phase 6 | Production Patterns & Real-World Integration | ⏳ Pending |

**Phase 6 ke baad aap RabbitMQ ke saare important concepts cover kar chuke honge.** Total 6 phases — abhi aap 4th mein ho, matlab **do phases baaki** hain.

Chaliye, ab Phase 4 shuru karte hain.

---

## 1. RPC (Remote Procedure Call) Pattern

### 1.1 Concept

RPC ek **request-response** pattern hai. Normally message queues **one-way** hote hain — producer bhejta hai, consumer leta hai, bas. Lekin kabhi kabhi aap chahte ho ki consumer **result wapas** bheje.

**Aasan analogy:** Phone call. Aap call karte ho, doosra banda uthata hai, aap sawaal poochho, woh jawab deta hai, phir call cut.

**RabbitMQ mein RPC kaise kaam karta hai:**
1. Client ek **request queue** pe message bhejta hai
2. Client ek **callback queue** bhi banata hai (reply ke liye)
3. Server request receive karta hai, process karta hai
4. Server result ko **callback queue** pe bhejta hai
5. Client result receive karta hai

### 1.2 Problem — Kaunsa Reply Kis Request Ka?

Agar ek client multiple requests bheje, toh reply kisi ko bhi mil sakta hai. Solution: **`correlation_id`**. Har request ke saath ek unique ID bheja jaata hai. Reply mein wahi ID aata hai. Client match karta hai.

### 1.3 Architecture

```
Client ──► [rpc_queue] ──► Server
   │                          │
   │                          ▼
   │                   Process request
   │                          │
   │                          ▼
   └◄── [callback_queue] ◄────┘
        (reply with correlation_id)
```

### 1.4 Complete Code Example

#### Server (`rpc_server.py`)

```python
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
```

#### Client (`rpc_client.py`)

```python
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
```

**Output:**
```
[x] Requesting fib(5)
[.] Got: 5
[x] Requesting fib(10)
[.] Got: 55
[x] Requesting fib(15)
[.] Got: 610
```

### 1.5 Kab Use Karein

✅ **Kab use karein:**
- Client ko **result chahiye** turant
- Synchronous behaviour chahiye
- Short-running tasks

❌ **Kab NA use karein:**
- Long-running tasks (block ho jaayega)
- Fire-and-forget chahiye
- Async processing chahiye

**⚠️ Warning:** RPC synchronous hai — client block rehta hai. Production mein timeouts zaroor set karo.

---

## 2. Clustering — Multiple Nodes

### 2.1 Concept

Ek RabbitMQ server crash ho jaaye toh poora system down. **Clustering** se multiple RabbitMQ **nodes** ek saath kaam karte hain.

**Aasan analogy:** Ek bank ki **multiple branches**. Agar ek branch band ho jaaye, baaki branches kaam karti rehti hain.

### 2.2 Cluster Ke Basics

```
        ┌──────────────────┐
        │  RabbitMQ Node 1 │
        │  (Master)        │
        └────────┬─────────┘
                 │
        ┌────────┴─────────┐
        │                  │
        ▼                  ▼
   ┌─────────┐       ┌─────────┐
   │ Node 2  │       │ Node 3  │
   └─────────┘       └─────────┘
```

**Key points:**
- Cluster mein **exchanges aur users** sab nodes pe share hote hain
- **Queues** by default sirf **ek node** pe hote hain (jahan declare hue)
- Clients kisi bhi node se connect kar sakte hain
- Agar koi node down ho, clients doosre node se connect kar sakte hain

### 2.3 Setup (Conceptual)

```bash
# Node 1 (master) start karo
rabbitmq-server -detached

# Node 2 ko cluster mein join karo
rabbitmqctl stop_app
rabbitmqctl join_cluster rabbit@node1
rabbitmqctl start_app

# Node 3 ko cluster mein join karo
rabbitmqctl stop_app
rabbitmqctl join_cluster rabbit@node1
rabbitmqctl start_app

# Cluster status dekho
rabbitmqctl cluster_status
```

### 2.4 Important Limitation

**Queue sirf ek node pe hoti hai** by default. Agar woh node crash ho jaaye, **queue unavailable** ho jaati hai.

**Solution:** Mirrored Queues ya Quorum Queues (next section).

---

## 3. High Availability — Mirrored & Quorum Queues

### 3.1 Mirrored Queues (Classic — Deprecated in 4.x)

Queue ka **master copy** ek node pe, aur **mirrors** doosre nodes pe.

```python
# Policy se mirrored queue banao
rabbitmqctl set_policy ha-all "^ha\." \
    '{"ha-mode":"all"}' \
    --apply-to queues
```

- **`ha-mode: all`** — saare nodes pe mirror
- **`ha-mode: exactly`** — specific number of mirrors
- **`ha-mode: nodes`** — specific nodes

**⚠️ Deprecated:** RabbitMQ 4.x mein mirrored queues **remove** kar diye gaye hain. Ab **Quorum Queues** use karo.

### 3.2 Quorum Queues (Modern — Recommended)

Quorum queue **Raft consensus algorithm** use karta hai. Queue ka data **majority nodes** pe replicate hota hai.

```python
# Quorum queue declare karo
channel.queue_declare(
    queue='critical_queue',
    durable=True,
    arguments={
        'x-queue-type': 'quorum'      # ⭐ Quorum type
    }
)
```

**Kaise kaam karta hai:**
- 3 nodes → 2 nodes pe data likho (majority)
- 1 node down ho → still works
- 2 nodes down → queue unavailable (majority loss)

**Fayde:**
- **Strong consistency** — Raft algorithm
- **Automatic failover** — leader crash, naya leader
- **Data safety** — majority pe likha, kho nahi sakta
- **Better performance** than mirrored

**Nuksaan:**
- **Zyaada resources** — 3+ nodes chahiye
- **Slightly slower** than classic queues
- **Larger messages** → performance impact

### 3.3 Quorum Queue vs Classic Queue

| Feature | Classic Queue | Quorum Queue |
|---|---|---|
| Replication | Mirrored (deprecated) | Raft-based |
| Consistency | Eventual | Strong |
| Data safety | Good | Excellent |
| Performance | Fastest | Good |
| Memory | Lower | Higher |
| Best for | Non-critical | Critical data |

**Rule:** Payment, order, critical data → Quorum queue. Logs, metrics → Classic queue.

---

## 4. Federation — Multi-Datacenter

### 4.1 Concept

**Federation** allow karta hai ki **do alag RabbitMQ brokers** (jaise Mumbai aur Delhi) aapas mein messages share karein.

**Aasan analogy:** Do post offices different cities mein. Federation ek **special courier** hai jo dono ke beech messages le jaata hai.

### 4.2 Federation vs Clustering

| Feature | Clustering | Federation |
|---|---|---|
| Location | Same datacenter | Different datacenters |
| Network | Low latency required | WAN tolerant |
| Consistency | Strong | Eventual |
| Setup | Complex | Simpler |
| Use case | HA within DC | Multi-region |

### 4.3 Setup (Conceptual)

```bash
# Upstream define karo (doosra broker)
rabbitmqctl set_parameter federation-upstream delhi-broker \
    '{"uri":"amqp://user:pass@delhi.example.com"}'

# Policy set karo — konsi queues federate karo
rabbitmqctl set_policy federate-orders "^orders\." \
    '{"federation-upstream":"delhi-broker"}' \
    --apply-to queues
```

### 4.4 Kab Use Karein

- Multi-region deployment
- Disaster recovery
- Data residency (data local rehna chahiye)
- WAN pe reliable messaging

---

## 5. Shovel — Cross-Broker Message Movement

### 5.1 Concept

**Shovel** ek **plugin** hai jo **do brokers ke beech messages** reliably move karta hai. Federation se zyada simple hai, aur **one-way** hai.

**Aasan analogy:** Shovel ek **pipe** hai jo ek bucket se doosre bucket mein paani (messages) le jaati hai.

### 5.2 Federation vs Shovel

| Feature | Federation | Shovel |
|---|---|---|
| Direction | Bidirectional | Unidirectional |
| Setup | Policy-based | Static config |
| Flexibility | Pattern-based | Fixed source/dest |
| Use case | Multi-DC | Data migration, DR |

### 5.3 Setup (Conceptual)

```bash
# Shovel enable karo
rabbitmq-plugins enable rabbitmq_shovel

# Shovel define karo
rabbitmqctl set_parameter shovel my-shovel \
    '{"src-uri":"amqp://localhost",
      "src-queue":"local_queue",
      "dest-uri":"amqp://remote.example.com",
      "dest-queue":"remote_queue"}'
```

### 5.4 Kab Use Karein

- **Data migration** — ek broker se doosre pe
- **Disaster recovery** — backup broker pe messages
- **Data aggregation** — multiple brokers se ek pe
- **Testing** — production messages ko test broker pe

---

## 6. Load Balancing — Multiple Consumers

### 6.1 Concept

Same queue pe **multiple consumers** — RabbitMQ messages **round-robin** distribute karta hai.

```
                    Queue "tasks"
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Consumer1  Consumer2  Consumer3
          msg1,msg4  msg2,msg5  msg3,msg6
```

### 6.2 Kaise Kaam Karta Hai

- **Prefetch = 0** (default) → Consumer jo free ho, usko next message
- **Prefetch = 1** → Fair distribution, slow consumer kam messages
- **Prefetch = 10** → Fast consumers zyada lete hain

### 6.3 Best Practice

```python
# Per consumer prefetch set karo
channel.basic_qos(prefetch_count=10)
```

**Multiple instances chalao:**
```bash
# Terminal 1
python worker.py

# Terminal 2
python worker.py

# Terminal 3
python worker.py
```

Sab same queue se consume karenge, aur RabbitMQ **fair distribution** karega.

### 6.4 Scaling Strategy

| Load | Solution |
|---|---|
| Low | 1 consumer |
| Medium | 3-5 consumers |
| High | 10+ consumers (horizontal scaling) |
| Very high | Multiple queues + multiple consumers per queue |

**Yeh **horizontal scaling** hai — jitne zyada consumers, utna zyada throughput.

---

## 7. Best Practices — Production Checklist

### 7.1 Producer Side

```python
✅ Publisher confirms enable karo
✅ delivery_mode=2 (persistent) for critical messages
✅ Retry logic with exponential backoff
✅ Connection pooling (ek connection, multiple channels)
✅ Graceful shutdown (connection.close())
✅ Handle exceptions properly
```

### 7.2 Consumer Side

```python
✅ auto_ack=False (manual ack always)
✅ basic_qos(prefetch_count=10) — fair distribution
✅ Try/except in callback
✅ On failure: basic_nack(requeue=True/False) based on error type
✅ Idempotent processing (duplicate delivery handle karo)
✅ Graceful shutdown (finish current message, then close)
```

### 7.3 Infrastructure

```bash
✅ Cluster (3+ nodes) for HA
✅ Quorum queues for critical data
✅ DLQ configured for all critical queues
✅ Message TTL set (avoid unbounded growth)
✅ Monitoring & alerts (queue depth, consumer count)
✅ Regular backups
✅ TLS/SSL for production connections
✅ Proper vhost separation per application
```

### 7.4 Anti-Patterns (Kya NAHI Karna)

```python
❌ auto_ack=True in production
❌ Infinite requeue (poison message loop)
❌ Non-durable queues for critical data
❌ Single consumer for high-throughput
❌ Prefetch=0 with slow processing
❌ Hardcoded credentials
❌ No monitoring
❌ One giant queue for everything
```

### 7.5 Monitoring Ke Metrics

| Metric | Kya dekhein |
|---|---|
| **Queue depth** | Zyada → consumer slow |
| **Consumer count** | Kam → scale karo |
| **Message rate** | Publish vs consume rate |
| **Unacked messages** | Zyada → processing stuck |
| **Connection count** | Leak detection |
| **Memory usage** | Leak detection |
| **Disk usage** | Persistence overhead |

---

## ✅ Phase 4 Ka Summary

Aaj humne seekha:

| # | Topic | Kya Samjha |
|---|---|---|
| 1 | **RPC Pattern** | Request-response messaging |
| 2 | **Clustering** | Multiple RabbitMQ nodes |
| 3 | **High Availability** | Quorum queues, Raft algorithm |
| 4 | **Federation** | Multi-datacenter messaging |
| 5 | **Shovel** | Cross-broker message movement |
| 6 | **Load Balancing** | Multiple consumers, prefetch |
| 7 | **Best Practices** | Production checklist |

**Key takeaways:**

1. **RPC** — jab response chahiye, correlation_id use karo
2. **Clustering** — 3+ nodes for HA
3. **Quorum queues** — critical data ke liye (mirrored deprecated)
4. **Federation** — multi-datacenter, eventual consistency
5. **Shovel** — one-way, simple cross-broker movement
6. **Horizontal scaling** — multiple consumers, prefetch
7. **Production** — durable + persistent + DLQ + monitoring

---

## 🎯 Phase 4 Complete — Ab Aage Kya?

Aapne ab tak **4 phases** complete kar liye. **Do phases baaki hain:**

### 📅 Phase 5: Management, Monitoring & Operations
Yeh phase cover karega:
- RabbitMQ Management UI (web interface)
- Command-line tools (`rabbitmqctl`, `rabbitmqadmin`)
- Monitoring metrics (Prometheus, Grafana)
- Logs analysis
- Backup & restore
- Upgrades aur maintenance
- Performance tuning
- Troubleshooting common issues

### 📅 Phase 6: Production Patterns & Real-World Integration
Yeh phase cover karega:
- RabbitMQ + Python frameworks (Celery, FastAPI, Django)
- Microservices patterns with RabbitMQ
- Event-driven architecture with RabbitMQ
- Saga pattern for distributed transactions
- Integration with Redis, Kafka
- Real case studies (e-commerce, notification system, payment processing)
- Comparison with Kafka, Redis, SQS
- Migration strategies

---

## 🎯 Ab Batao

Phase 4 complete! Aapne RabbitMQ ke **saare advanced features** cover kar liye.

**Ab aap kya karna chahte ho?**

1. **Phase 5 shuru karein** — Management, Monitoring & Operations
2. **Phase 6 shuru karein** — Production Patterns & Real-World Integration
3. **Practical setup** — RabbitMQ install karke saare concepts apne haathon se chalao
4. **Kuch questions** — Agar koi concept clear nahi hua

Batao, kya karna hai next? 🚀