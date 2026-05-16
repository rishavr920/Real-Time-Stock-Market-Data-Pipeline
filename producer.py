import time
import json
import random
from datetime import datetime, timezone
# pyrefly: ignore [missing-import]
from kafka import KafkaProducer

# 1: KAFKA PRODUCER SETUP
# This producer will connect to Kafka running on the local Mac.
# 'value_serializer' converts the data into JSON and encodes it into bytes before sending,
# because Kafka only understands bytes.

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Name of the Kafka topic. If this topic does not exist, Kafka will automatically create it.KAFKA_TOPIC = 'stock-events'

# Kuch famous companies ke stock symbols
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NFLX', 'NVDA', 'BABA', 'V']

# 2: FAKE DATA GENERATOR
def generate_stock_tick():
    """Har call par ek naya fake stock price generate karta hai."""
    symbol = random.choice(SYMBOLS)
    
    # Base price will be random between 50 se 1000.
    base_price = round(random.uniform(50.0, 1000.0), 2)
    
    # For Price fluctuation
    fluctuation = round(random.uniform(-2.0, 2.0), 2)
    final_price = round(base_price + fluctuation, 2)
    
    # Final data dictionary (in JSON formatting)
    event = {
        'symbol': symbol,
        'price': final_price,
        'event_time': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z') # Real-time timestamp (UTC)
    }
    return event

# STEP 3: MAIN LOOP (Continuous Sending)
if __name__ == "__main__":
    print("Starting Stock Producer...")
    print(f"Sending real-time data to Kafka Topic: '{KAFKA_TOPIC}'")
    print("Press Ctrl+C to stop.")
    print("-" * 50)
    
    try:
        while True:
            # 1. creating fake data.
            stock_event = generate_stock_tick()
            
            # 2. sending data to kafka
            producer.send(KAFKA_TOPIC, value=stock_event)
        
            print(f"Sent → {stock_event}")
            
            # 4. Delay of 0.5 seconds (to simulate 2 messages per second)
            time.sleep(0.5) 
            
    except KeyboardInterrupt:
        # When we press Ctrl+C, the program will shut down gracefully.
        print("\n🛑 Stopping producer...")
        producer.close()
        print("✅ Producer stopped safely.")
