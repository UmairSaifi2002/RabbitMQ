# RabbitMQ — Phase 3: Reliability & Message Acknowledgments

Aaiye, ab hum RabbitMQ ka **sabse important** phase shuru karte hain — **Reliability**. Phase 1 aur 2 mein humne messaging ka basic flow aur routing samjha. Lekin **asli production** mein sabse bada sawaal yeh hota hai:

> **"Message kho na jaaye — chahe kuch bhi ho jaaye."**

Phase 3 mein hum yeh guarantee kaise banate hain, woh seekhenge.

**Phase 3 mein hum yeh cover karenge:**

1. Reliability kya hai — problem statement
2. Message Acknowledgment (Consumer side)
3. Message Durability (Broker side)
4. Publisher Confirms (Producer side)
5. Prefetch / QoS (Flow control)
6. Dead Letter Queue (Failure handling)
7. Message TTL & Queue TTL (Expiry)
8. Priority Queues (Ordering)
9. Putting it all together — production-ready example

---

## 1. Reliability Kya Hai — Problem Statement

Socho ek **payment processing system**. User ne payment kiya. Message broker mein aaya. Ab dekho **kahan-kahan message kho sakta hai**:

```
Producer → Broker → Queue → Consumer → Processing
   ↑         ↑        ↑        ↑           ↑
 Loss 1    Loss 2   Loss 3   Loss 4      Loss 5
```

| # | Kahan | Kya ho sakta hai |
|---|---|---|
| 1 | Producer → Broker | Network fail, broker down |
| 2 | Broker (memory) | Broker crash, memory se gaya |
| 3 | Queue (RAM) | Server restart, queue gaya |
| 4 | Consumer pickup | Consumer crash after reading |
| 5 | Processing | Exception during processing |

**RabbitMQ mein har loss ke liye solution hai:**

| Loss | Solution |
|---|---|
| 1 | Publisher Confirms |
| 2, 3 | Durable Queues + Persistent Messages |
| 4 | Manual Acknowledgment |
| 5 | Dead Letter Queue + Retry |

Chalo har ek ko detail mein samjhte hain.

---

## 2. Message Acknowledgment (Consumer Side)

### 2.1 Concept

Jab consumer message **receive** karta hai, RabbitMQ ko **turant** nahi batata ki "ho gaya". Consumer **khud** decide karta hai ki kab RabbitMQ ko batana hai.

**Do modes:**

| Mode | Kya karta hai | Risk |
|---|---|---|
| **Auto-Ack** (`auto_ack=True`) | Message deliver hote hi "done" maan lo | Consumer crash ho gaya → message gaya |
| **Manual-Ack** (default) | Consumer khud `basic_ack()` call kare | Safe — crash ho gaya toh message redeliver hoga |

### 2.2 Auto-Ack Ka Problem

```python
channel.basic_consume(
    queue='payment_queue',
    on_message_callback=callback,
    auto_ack=True         # ⚠️ Message turant ack ho gaya
)

def callback(ch, method, properties, body):
    process_payment(body)   # ❌ Agar yahan crash ho gaya, message GAYA
```

**Kya hota hai:**
1. Message consumer ko mila
2. **Turant** RabbitMQ ko "done" mila (auto_ack)
3. Message queue se **delete** ho gaya
4. `process_payment` fail ho gaya
5. **Message permanently GAYA** — koi retry nahi

**Yeh payment system ke liye disaster hai.**

### 2.3 Manual Ack — Sahi Tarika

```python
channel.basic_consume(
    queue='payment_queue',
    on_message_callback=callback,
    auto_ack=False        # ✅ Manual ack
)

def callback(ch, method, properties, body):
    try:
        process_payment(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)   # ✅ Success pe ack
    except Exception as e:
        # ❌ Failure pe ack NAHI karo
        # Message queue mein wapas jaayega (redelivery)
        print(f"Failed: {e}")
        # Option 1: Chhod do (redeliver hoga)
        # Option 2: basic_nack() se reject karo
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
```

**Kya hota hai:**
1. Message consumer ko mila
2. Consumer **delivery_tag** note karta hai
3. `process_payment` chalta hai
4. **Success** → `basic_ack()` → RabbitMQ message delete karta hai
5. **Failure** → `basic_nack()` → RabbitMQ message **requeue** karta hai (ya DLQ mein bhejta hai)

### 2.4 `basic_ack` vs `basic_nack` vs `basic_reject`

| Command | Kya karta hai |
|---|---|
| `basic_ack(delivery_tag)` | Success — message delete karo |
| `basic_nack(delivery_tag, requeue=True/False)` | Fail — requeue karo ya discard |
| `basic_reject(delivery_tag, requeue=True/False)` | Same as nack (ek message ke liye) |

**`nack` vs `reject`:**
- `reject` — ek message reject karta hai
- `nack` — multiple messages reject kar sakta hai (RabbitMQ extension)

### 2.5 Complete Code Example

#### Consumer with Manual Ack (`consumer_ack.py`)

```python
import pika
import time
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks', durable=True)

def callback(ch, method, properties, body):
    task = body.decode()
    print(f" [x] Received: {task}")
    
    try:
        # Simulate work
        time.sleep(1)
        
        # 20% chance failure
        if random.random() < 0.2:
            raise Exception("Processing failed")
        
        print(f" [✓] Done: {task}")
        ch.basic_ack(delivery_tag=method.delivery_tag)   # ✅ Success
        
    except Exception as e:
        print(f" [✗] Failed: {task} — {e}")
        # Requeue → doosra consumer try karega
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

channel.basic_qos(prefetch_count=1)   # Ek baar mein 1 message (baad mein samjhenge)
channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting for tasks...')
channel.start_consuming()
```

**Agar consumer crash ho jaaye processing ke beech:**
- Message **ack nahi hua**
- RabbitMQ **timeout** ke baad message **requeue** karta hai
- Doosra consumer uthaega
- **Message kho nahi jaata**

---

## 3. Message Durability (Broker Side)

### 3.1 Concept

Consumer side pe ack se message safe ho gaya. Lekin **broker khud crash ho jaaye toh?**

**Teen cheezein durable honi chahiye:**

| Cheez | Durable banane ka tarika |
|---|---|
| **Exchange** | `durable=True` declare karte waqt |
| **Queue** | `durable=True` declare karte waqt |
| **Message** | `delivery_mode=2` properties mein |

### 3.2 Durable Exchange & Queue

```python
# Durable exchange
channel.exchange_declare(
    exchange='orders',
    exchange_type='direct',
    durable=True         # ✅ Server restart pe bhi rahe
)

# Durable queue
channel.queue_declare(
    queue='order_queue',
    durable=True         # ✅ Server restart pe bhi rahe
)
```

**⚠️ Important:** `durable` sirf **declaration ke waqt** set hoti hai. Agar queue pehle se exist karti hai with different settings, RabbitMQ **error** dega.

### 3.3 Persistent Message

```python
channel.basic_publish(
    exchange='orders',
    routing_key='order.created',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2      # ✅ 2 = persistent (disk pe likho)
    )
)
```

**`delivery_mode`:**
- `1` = transient (RAM mein, restart pe gaya)
- `2` = persistent (disk pe, restart pe bhi rahe)

### 3.4 ⚠️ Trade-off — Performance

**Persistent messages slow hote hain** kyunki disk pe likhna padta hai.

| Setup | Speed | Durability |
|---|---|---|
| Non-durable queue + transient message | ⚡ Fastest | ❌ Restart pe gaya |
| Durable queue + transient message | ⚡ Fast | ⚠️ Queue rahegi, message nahi |
| Durable queue + persistent message | 🐢 Slower | ✅ Full durability |

**Rule:** Payment, order, critical data → always durable + persistent. Logs, metrics → transient chalega.

### 3.5 Complete Code Example

#### Producer (`durable_producer.py`)

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Durable exchange
channel.exchange_declare(
    exchange='critical',
    exchange_type='direct',
    durable=True
)

# Durable queue
channel.queue_declare(queue='critical_queue', durable=True)

# Bind
channel.queue_bind(
    exchange='critical',
    queue='critical_queue',
    routing_key='critical'
)

# Persistent message
channel.basic_publish(
    exchange='critical',
    routing_key='critical',
    body='Payment #12345',
    properties=pika.BasicProperties(
        delivery_mode=2      # ⭐ Persistent
    )
)

print(" [x] Sent persistent message")
connection.close()
```

**Ab agar RabbitMQ server restart ho jaaye**, message **safe rahega** (disk pe likha hai).

---

## 4. Publisher Confirms (Producer Side)

### 4.1 Concept

Ab tak humne **consumer side** aur **broker side** dekha. Lekin ek **gap** bacha:

> **Producer ne message bheja — lekin broker ko mila ya nahi, producer ko kaise pata chale?**

**Publisher Confirms** yeh solution hai. Yeh **TCP acknowledgment ke upar** ek layer hai. Jab broker message **safely store** kar leta hai, toh producer ko **confirm** bhejta hai.

### 4.2 Do Modes

#### Mode 1: Simple Confirm (Synchronous)

```python
channel.confirm_delivery()   # Enable confirms

try:
    channel.basic_publish(
        exchange='orders',
        routing_key='order.created',
        body=message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    print("✅ Broker confirmed")
except pika.exceptions.UnroutableError:
    print("❌ Message not routed")
except pika.exceptions.NackError:
    print("❌ Broker nacked the message")
```

**Yeh block karta hai** jab tak confirm na aaye — slow.

#### Mode 2: Batch Confirm (Asynchronous, Fast)

```python
# Multiple messages bhejo, phir sabke confirms ka wait karo
confirmed = 0
for i in range(1000):
    channel.basic_publish(...)
    confirmed += 1

channel.wait_for_confirms()   # Sab confirm hone ka wait
```

### 4.3 Publisher Confirm Ka Fayda

**Bina confirm:**
- Producer message bhejta hai
- Broker crash ho jaata hai before storing
- Producer ko **kuch nahi pata**
- **Message permanently GAYA**

**Confirm ke saath:**
- Producer message bhejta hai
- Broker disk pe likhta hai
- Broker **confirm** bhejta hai
- Producer **satisfied** — "message safe hai"
- Agar confirm **nahi aaya** → producer **retry** kar sakta hai

### 4.4 Complete Code Example

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='confirm_queue', durable=True)
channel.confirm_delivery()      # ⭐ Enable publisher confirms

for i in range(5):
    try:
        channel.basic_publish(
            exchange='',
            routing_key='confirm_queue',
            body=f'Message {i}',
            properties=pika.BasicProperties(delivery_mode=2)
        )
        print(f"✅ Message {i} confirmed by broker")
    except pika.exceptions.AMQPError as e:
        print(f"❌ Message {i} failed: {e}")

connection.close()
```

---

## 5. Prefetch / QoS (Flow Control)

### 5.1 Concept

Socho ek consumer ne **1000 messages** ek saath utha liye. Lekin processing slow hai. Beech mein consumer crash ho gaya. **Saare 1000 messages wapas queue mein jaayenge** — doosra consumer sab process karega. Yeh **wasteful** hai.

**Solution:** `prefetch_count` — "ek baar mein kitne messages de do."

### 5.2 Kaise Kaam Karta Hai

```python
channel.basic_qos(prefetch_count=1)   # Ek baar mein sirf 1 message
```

**Flow:**
1. Consumer message #1 leta hai
2. Broker **wait** karta hai jab tak ack na aaye
3. Consumer ack deta hai
4. Broker message #2 bhejta hai
5. Repeat...

### 5.3 Prefetch Values

| Value | Behaviour |
|---|---|
| `0` | **Unlimited** (default — sab messages ek saath) |
| `1` | Ek baar mein ek — **safest**, slow |
| `10` | 10 ek saath — **balanced** |
| `100` | 100 ek saath — fast, but risk |

**Real-world:** Usually **10-50** use karte hain. Balance milta hai speed aur safety mein.

### 5.4 Complete Code Example

```python
import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='tasks', durable=True)

# ⭐ Prefetch: ek baar mein 1 message
channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    print(f" [x] Received: {body.decode()}")
    time.sleep(2)   # Slow processing
    print(f" [✓] Done: {body.decode()}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting...')
channel.start_consuming()
```

**Agar 3 consumers chalao with prefetch=1:**
- Broker har consumer ko **ek-ek** message dega
- Fair distribution hoga
- Slow consumer sirf ek message le raha hoga, baaki consumers busy rahenge

### 5.5 Prefetch Ke Bina — Problem

```
Without prefetch:
  Consumer 1: [msg1, msg2, msg3, ..., msg100]  ← Sab le liya
  Consumer 2: [idle]
  Consumer 3: [idle]

With prefetch=1:
  Consumer 1: [msg1]  ← Ek
  Consumer 2: [msg2]  ← Ek
  Consumer 3: [msg3]  ← Ek
  (Jab ek ack karta hai, tab agla milta hai)
```

**Yeh load balancing ka asli tareeka hai.**

---

## 6. Dead Letter Queue (DLQ)

### 6.1 Concept

Kuch messages **kabhi process nahi ho paate** — jaise:
- Invalid format
- Missing fields
- Baar-baar fail hone wale (poison messages)

Aise messages ko **kya karein?** Infinite retry karna **wasteful** hai. Unko **alag queue mein daal do** — jise **Dead Letter Queue** kehte hain.

### 6.2 Kaise Kaam Karta Hai

```
Main Queue "orders" ──► Consumer
                         │
                         ├── ✅ Success → ack
                         │
                         └── ❌ Failure (multiple times) → DLQ "orders.dlq"
                                                            │
                                                            └── Manual review / alerts
```

### 6.3 DLQ Ke Triggers

Message DLQ mein jaata hai jab:
1. Consumer **`basic_nack(requeue=False)`** karta hai
2. Message **TTL expire** ho jaata hai
3. Queue **length limit** hit karti hai
4. Message **reject** hota hai

### 6.4 Complete Code Example

#### Setup — Main Queue + DLQ

```python
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
```

#### Consumer with DLQ (`dlq_consumer.py`)

```python
import pika
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    print(f" [x] Processing: {body.decode()}")
    
    if random.random() < 0.5:
        # ❌ Fail — DLQ mein bhejo
        print(f" [✗] Failed → DLQ")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)  # ⭐ requeue=False
    else:
        # ✅ Success
        print(f" [✓] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='orders', on_message_callback=callback, auto_ack=False)

print(' [*] Waiting...')
channel.start_consuming()
```

#### Monitor DLQ (`dlq_monitor.py`)

```python
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def callback(ch, method, properties, body):
    print(f" 💀 [DLQ] Failed message: {body.decode()}")
    # Yahan aap alert bhej sakte ho, ya manually review karo
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='orders_dlq', on_message_callback=callback, auto_ack=False)

print(' [*] Monitoring DLQ...')
channel.start_consuming()
```

### 6.5 DLQ Ke Fayde

- **Infinite retry se bachao** — poison messages alag
- **Manual review** — baad mein dekho
- **Alerts** — DLQ mein kuch aaya toh notify karo
- **Debugging** — failing messages ka record

---

## 7. Message TTL & Queue TTL (Expiry)

### 7.1 Message TTL

Message **kitni der tak queue mein rahe**. Uske baad **automatically delete** ho jaayega.

**Per-message TTL:**
```python
channel.basic_publish(
    exchange='',
    routing_key='queue',
    body='Message',
    properties=pika.BasicProperties(
        expiration='60000'    # 60 seconds (string, milliseconds)
    )
)
```

**Per-queue TTL (sab messages ke liye):**
```python
channel.queue_declare(
    queue='ttl_queue',
    arguments={
        'x-message-ttl': 60000    # 60 seconds
    }
)
```

### 7.2 Queue TTL (Auto-Delete)

Queue **kitni der tak alive rahe** agar koi use nahi kar raha:

```python
channel.queue_declare(
    queue='temp_queue',
    arguments={
        'x-expires': 1800000    # 30 minutes
    }
)
```

### 7.3 Kaise Kaam Karta Hai

| TTL | Kya hota hai jab expire |
|---|---|
| **Message TTL expire** | Message DLQ mein jaata hai (agar DLX set hai) ya delete |
| **Queue TTL expire** | Poora queue delete ho jaata hai |

### 7.4 Use Cases

- **Message TTL** — OTP codes, session tokens, time-sensitive data
- **Queue TTL** — Temporary queues, RPC reply queues

---

## 8. Priority Queues

### 8.1 Concept

Kuch messages **urgent** hote hain, kuch **normal**. Priority queue se urgent messages **pehle** process hote hain.

### 8.2 Setup

```python
channel.queue_declare(
    queue='priority_queue',
    arguments={
        'x-max-priority': 10     # 0-10 priority levels
    }
)
```

### 8.3 Message Ke Saath Priority Bhejo

```python
channel.basic_publish(
    exchange='',
    routing_key='priority_queue',
    body='Urgent task',
    properties=pika.BasicProperties(
        priority=9              # 9 = high priority
    )
)

channel.basic_publish(
    exchange='',
    routing_key='priority_queue',
    body='Normal task',
    properties=pika.BasicProperties(
        priority=1              # 1 = low priority
    )
)
```

### 8.4 Kaise Kaam Karta Hai

- **Higher priority** wale messages **pehle** deliver hote hain
- Same priority wale **FIFO** order mein
- **Range:** 0-255 (but practical: 0-10)

**⚠️ Warning:** Priority queue **performance hit** karti hai. Sirf zaroorat ho toh use karo.

---

## 9. Putting It All Together — Production-Ready Example

Chalo ek **complete payment processing system** banate hain jo **saari reliability features** use kare.

### Architecture

```
Producer ──► Exchange (durable) ──► Queue (durable, DLX, TTL)
                                        │
                                        ├── Consumer (manual ack, prefetch, DLQ)
                                        │
                                        └── DLQ (failed messages)
```

### Producer (`payment_producer.py`)

```python
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
```

### Consumer (`payment_consumer.py`)

```python
import pika
import json
import time
import random

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Prefetch — ek baar mein 1 message
channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    payment = json.loads(body)
    print(f" [x] Processing payment #{payment['id']} (${payment['amount']})")
    
    try:
        # Simulate processing
        time.sleep(1)
        
        # Random failure simulation
        if random.random() < 0.3:
            raise Exception("Payment gateway timeout")
        
        print(f" [✓] Payment #{payment['id']} success")
        ch.basic_ack(delivery_tag=method.delivery_tag)     # ✅ Ack
        
    except Exception as e:
        print(f" [✗] Payment #{payment['id']} failed: {e}")
        # ❌ Fail → requeue=False → DLQ mein jaayega
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='payments_queue', on_message_callback=callback, auto_ack=False)

print(' [*] Payment processor waiting...')
channel.start_consuming()
```

### DLQ Monitor (`payment_dlq_monitor.py`)

```python
import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def callback(ch, method, properties, body):
    payment = json.loads(body)
    print(f" 💀 [DLQ] Payment #{payment['id']} needs manual review")
    # Yahan alert bhej sakte ho, dashboard update kar sakte ho
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='payments_dlq', on_message_callback=callback, auto_ack=False)

print(' [*] DLQ monitor running...')
channel.start_consuming()
```

### Reliability Matrix

| Loss Point | Solution Used |
|---|---|
| Producer → Broker | `confirm_delivery()` |
| Broker crash | `durable=True` on exchange |
| Queue lost | `durable=True` on queue |
| Message lost | `delivery_mode=2` |
| Consumer crash | `auto_ack=False` |
| Slow consumer | `prefetch_count=1` |
| Poison messages | DLQ with `requeue=False` |
| Old messages | `x-message-ttl` |

---

## ✅ Phase 3 Ka Summary

Aaj humne seekha:

| # | Topic | Kya Samjha |
|---|---|---|
| 1 | Reliability | Message kahan-kahan kho sakta hai |
| 2 | **Manual Ack** | Consumer khud bataye "done" |
| 3 | **Durability** | Durable exchange, queue, persistent message |
| 4 | **Publisher Confirms** | Producer ko pata chale message safe hai |
| 5 | **Prefetch/QoS** | Consumer ek baar mein kitne messages le |
| 6 | **Dead Letter Queue** | Failed messages alag queue mein |
| 7 | **Message TTL** | Messages ka expiry |
| 8 | **Queue TTL** | Queue ka expiry |
| 9 | **Priority Queues** | Urgent messages pehle |
| 10 | Complete example | Production-ready payment system |

**Key takeaways:**

1. **Auto-ack mat use karo production mein** — manual ack always
2. **Durable + Persistent** — critical data ke liye zaroori
3. **Publisher confirms** — producer ko confirmation
4. **Prefetch** — fair distribution, safe
5. **DLQ** — poison messages alag karo
6. **TTL** — stale data automatic delete
7. **Reliability = multiple layers** — ek bhi layer skip na karo

---

## 🎯 Ab Batao

Phase 3 complete! Aapne RabbitMQ ke **saare reliability features** cover kar liye.

**Ab aap kya karna chahte ho?**

1. **Phase 4 shuru karein** — Advanced Topics (RPC pattern, Federation, Shovel, Clustering, High Availability)
2. **Practical implementation** — RabbitMQ install karke saare concepts apne haathon se chalao
3. **Kuch questions** — Agar koi concept clear nahi hua
4. **Real-world deep dive** — Specific use cases pe detail (payment system, notification system, etc.)

Batao, kya karna hai next? 🚀