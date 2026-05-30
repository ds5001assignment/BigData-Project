import json
import time
import random
import uuid
from kafka import KafkaProducer
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers=['100.84.105.9:9092'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'ecommerce-events'
event_count = 0

event_types = ["page_view", "search", "add_to_cart", "checkout_start", "payment"]
# Heavy on browsing, lighter on payments (mimics a real conversion funnel)
event_weights = [50, 25, 15, 7, 3] 

categories = ["Electronics", "Apparel", "Home & Garden", "Beauty", "Sports"]
devices = ["Mobile-iOS", "Mobile-Android", "Desktop-Mac", "Desktop-Windows", "Tablet"]
locations = ["US", "UK", "PK", "CA", "AE", "AU"]
servers = ["node-A", "node-B", "node-C"]
gateways = ["Stripe", "PayPal", "CyberSource"]

print("Starting Enterprise E-Commerce Data Generator...")
print("Broadcasting to Tailscale IP: 100.84.105.9\n")

try:
    while True:
        event_count += 1
        
        # 1. ALWAYS generate a random user action FIRST
        event = random.choices(event_types, weights=event_weights)[0]
        device = random.choice(devices)
        location = random.choice(locations)
        server = random.choice(servers)
        category = random.choice(categories)
        gateway = random.choice(gateways) if event == "payment" else "N/A"
        
        cart_amount = round(random.uniform(10.0, 500.0), 2) if event in ["add_to_cart", "checkout_start", "payment"] else 0.0

        # Default normal behavior
        status = "success"
        response_time = random.randint(15, 120)
        is_anomaly = False
        
        cycle = event_count % 400 

       # --- 2. PROBABILISTIC INCIDENT WINDOWS ---
        cycle = event_count % 2000 
        
        # INCIDENT 1: Gateway Degradation (Events 100-400)
        if 100 <= cycle <= 400:
            if random.random() < 0.25:
                event = "payment"
                server = "node-B" # <--- Concentrated Blast
                gateway = "Stripe"
                status = "failed"
                cart_amount = round(random.uniform(800.0, 1500.0), 2) 
                response_time = random.randint(1500, 3000)
                is_anomaly = True
                print(f"🚨 [CRITICAL] {gateway} Timeout on {server}! | Risk: ${cart_amount}")

        # INCIDENT 2: Apparel Add-to-Cart Glitch (Events 600-900)
        elif 600 <= cycle <= 900:
            if random.random() < 0.40:
                event = "add_to_cart"
                server = "node-A" # <--- Concentrated Blast
                category = "Apparel"
                status = "failed"
                response_time = random.randint(10, 30)
                is_anomaly = True
                print(f"⚠️ [HIGH] 'Add to Cart' Glitch on {server} ({category}) | Fails instantly.")

        # INCIDENT 3: Checkout Funnel Disruption (Events 1100-1400)
        elif 1100 <= cycle <= 1400:
            if random.random() < 0.35:
                event = "checkout_start"
                server = "node-C" # <--- Concentrated Blast
                status = "failed"
                response_time = random.randint(50, 200)
                is_anomaly = True
                print(f"⚠️ [HIGH] Checkout Funnel Disruption on {server} | Flow Blocked.")

        # INCIDENT 4: Search API Jitter (Events 1600-1900)
        elif 1600 <= cycle <= 1900:
            if random.random() < 0.50:
                event = "search"
                server = "node-B" # <--- Concentrated Blast
                status = "success"
                response_time = random.randint(800, 2500) 
                is_anomaly = True
                print(f"🔍 [WARNING] Search API Jitter on {server} | Latency: {response_time}ms")

        # --- 3. NORMAL TRAFFIC FALLBACK ---
        if not is_anomaly:
            # Base failure rate for healthy traffic
            if event == "payment":
                status = random.choices(["success", "failed"], weights=[95, 5])[0]
            else:
                status = random.choices(["success", "failed"], weights=[99, 1])[0]
            
            print(f"✅ [{server}] {event.upper().ljust(15)} | Gateway: {gateway.ljust(11)} | Status: {status.ljust(7)} | {response_time}ms")

        # 4. SEND TO KAFKA
        event_data = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": f"usr_{random.randint(10000, 99999)}",
            "session_id": f"sess_{random.randint(1000, 9999)}",
            "client_context": {"device_type": device, "location": location},
            "server_node": server,
            "event_type": event,
            "product_context": {"category": category, "cart_amount_usd": cart_amount},
            "payment_gateway": gateway,
            "status": status,
            "response_time_ms": response_time
        }

        producer.send(topic_name, value=event_data)
        time.sleep(random.uniform(0.01, 0.1)) # Slightly randomized pause between events

except KeyboardInterrupt:
    print("\nGenerator stopped.")
finally:
    producer.close()