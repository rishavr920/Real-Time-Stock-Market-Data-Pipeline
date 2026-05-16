import time
import json
import random
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from kafka import KafkaProducer

# ==========================================
# STEP 1: KAFKA PRODUCER SETUP
# ==========================================
# Ye producer Kafka se connect karega jo local Mac par run ho raha hai.
# 'value_serializer' data ko bhejte waqt JSON mein convert karke bytes mein encode karta hai, 
# kyunki Kafka sirf bytes samajhta hai.

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka Topic ka naam. Agar ye topic nahi hai, toh Kafka isko automatically bana dega.
KAFKA_TOPIC = 'stock-events'

# Kuch famous companies ke stock symbols
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NFLX', 'NVDA', 'BABA', 'V']

# ==========================================
# STEP 2: FAKE DATA GENERATOR
# ==========================================
def generate_stock_tick():
    """Har call par ek naya fake stock price generate karta hai."""
    symbol = random.choice(SYMBOLS)
    
    # Base price random 50 se 1000 ke beech mein
    base_price = round(random.uniform(50.0, 1000.0), 2)
    
    # Price fluctuation (Thoda upar ya neeche)
    fluctuation = round(random.uniform(-2.0, 2.0), 2)
    final_price = round(base_price + fluctuation, 2)
    
    # Final data dictionary (JSON ban jayega)
    event = {
        'symbol': symbol,
        'price': final_price,
        'event_time': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z') # Real-time timestamp (UTC)
    }
    return event

# ==========================================
# STEP 3: MAIN LOOP (Continuous Sending)
# ==========================================
if __name__ == "__main__":
    print("🚀 Starting Stock Producer...")
    print(f"📡 Sending real-time data to Kafka Topic: '{KAFKA_TOPIC}'")
    print("🛑 Press Ctrl+C to stop.")
    print("-" * 50)
    
    try:
        while True:
            # 1. Data banayein
            stock_event = generate_stock_tick()
            
            # 2. Data Kafka ko bhejein
            producer.send(KAFKA_TOPIC, value=stock_event)
            
            # 3. Screen par print karein taaki hume dikhe kya ho raha hai
            print(f"Sent → {stock_event}")
            
            # 4. 0.5 seconds ka gap (2 messages per second simulate karne ke liye)
            time.sleep(0.5) 
            
    except KeyboardInterrupt:
        # Jab hum Ctrl+C dabayenge toh ye clean tarike se band hoga
        print("\n🛑 Stopping producer...")
        producer.close()
        print("✅ Producer stopped safely.")
