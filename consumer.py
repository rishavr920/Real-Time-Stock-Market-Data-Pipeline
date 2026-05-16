import os
import json
import uuid
from datetime import datetime
import pandas as pd
import boto3
# pyrefly: ignore [missing-import]
from kafka import KafkaConsumer
# pyrefly: ignore [missing-import]
from dotenv import load_dotenv

# ==========================================
# STEP 1: SETUP AWS & ENVIRONMENT
# ==========================================
# Load environment variables from .env file
load_dotenv()

# Set up S3 Client (Ye S3 pe data bhejega)
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'ap-south-1')
)
S3_BUCKET = os.getenv('S3_BUCKET_NAME')

# ==========================================
# STEP 2: SETUP KAFKA CONSUMER
# ==========================================
KAFKA_TOPIC = 'stock-events'

# Interview Tip: S3 mein bahut saari chhoti 1-2 KB ki files banana bad practice hai.
# Isliye hum "Micro-batching" use karte hain. 50 messages ka ek batch banayenge, phir S3 par bhejenge.
BATCH_SIZE = 50 

print(f"🔄 Starting Consumer... Listening to '{KAFKA_TOPIC}'")
print(f"📂 Messages will be batched and saved to S3 bucket: {S3_BUCKET}")
print("-" * 50)

# Create consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest', # Start reading from the beginning if no offset is present
    enable_auto_commit=False,     # FAULT TOLERANCE: We commit manually ONLY after successfully saving to S3
    group_id='stock-processor-group'
)

# ==========================================
# STEP 3: READ, BATCH, AND UPLOAD
# ==========================================
def upload_to_s3(dataframe):
    """Pandas DataFrame ko Parquet mein convert karke S3 pe upload karta hai"""
    
    # Har batch ke liye unique filename banate hain timestamp ke sath
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = str(uuid.uuid4())[:8] # Random 8 character string
    
    # S3 file path (folder structure create ho jayega: raw_data/)
    s3_key = f"raw_data/stock_events_{timestamp}_{unique_id}.parquet"
    
    # Save dataframe as Parquet locally temporarily in /tmp/ folder
    temp_file = f"/tmp/stock_events_{timestamp}.parquet"
    
    # Parquet format is super compressed and optimized for AWS Athena queries
    dataframe.to_parquet(temp_file, index=False)
    
    # Upload to AWS S3
    try:
        print(f"☁️  Uploading to S3...")
        s3_client.upload_file(temp_file, S3_BUCKET, s3_key)
        print(f"✅ Success! Uploaded {len(dataframe)} records to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"❌ Error uploading to S3: {e}")
        # Agar error aya, toh aage continue nahi karenge, raise kar denge
        raise e
    finally:
        # Upload hone ke baad local temporary file delete kar do
        if os.path.exists(temp_file):
            os.remove(temp_file)

if __name__ == "__main__":
    messages_batch = []
    
    try:
        # Loop continuesly Kafka se data read karega
        for message in consumer:
            # Message value me humara JSON payload hota hai
            event = message.value
            messages_batch.append(event)
            
            # Screen par print karo
            print(f"Received → {event['symbol']}: ${event['price']}")
            
            # Agar batch full ho gaya (e.g. 50 messages) -> Save to S3
            if len(messages_batch) >= BATCH_SIZE:
                print(f"\n📦 Batch full ({BATCH_SIZE} records). Processing...")
                
                # Convert list of dictionaries into Pandas DataFrame
                df = pd.DataFrame(messages_batch)
                
                # Upload to S3 (this function handles conversion to Parquet and upload)
                upload_to_s3(df)
                
                # Commit offset manually! 
                # Ye bata raha hai Kafka ko ki humne ye messages successfully process kar liye hain.
                # Agar upload_to_s3 me error aata, toh code crash ho jata aur commit nahi hota.
                # Iska matlab consumer restart hone pe wapas unhi messages ko read karta (Zero Data Loss!)
                consumer.commit()
                
                # Clear the batch for the next round
                messages_batch.clear()
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping Consumer...")
        consumer.close()
        print("✅ Consumer stopped safely.")
