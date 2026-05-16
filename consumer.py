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

# STEP 1: SETUP AWS & ENVIRONMENT
# Load environment variables from .env file
load_dotenv()

# Set up S3 Client (This will send data to S3)
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION', 'ap-south-1')
)
S3_BUCKET = os.getenv('S3_BUCKET_NAME')

# 2: SETUP KAFKA CONSUMER
KAFKA_TOPIC = 'stock-events'

# Interview Tip: Creating many small 1-2 KB files in S3 is a bad practice.
# That's why we use "Micro-batching". We'll create a batch of 50 messages, then send to S3.
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

# STEP 3: READ, BATCH, AND UPLOAD

def upload_to_s3(dataframe):
    """Convert Pandas DataFrame to Parquet and upload to S3"""
    
    # Create a unique filename for each batch with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    unique_id = str(uuid.uuid4())[:8] # Random 8 character string
    
    # S3 file path (folder structure will be created: raw_data/)
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
        # If error occurs, we won't continue further, we will raise the error
        raise e
    finally:
        # Delete local temporary file after upload
        if os.path.exists(temp_file):
            os.remove(temp_file)

if __name__ == "__main__":
    messages_batch = []
    
    try:
        # Loop continuously to read data from Kafka
        for message in consumer:
            # Message value contains our JSON payload
            event = message.value
            messages_batch.append(event)
            
            # Print to screen
            print(f"Received → {event['symbol']}: ${event['price']}")
            
            # If batch is full (e.g. 50 messages) -> Save to S3
            if len(messages_batch) >= BATCH_SIZE:
                print(f"\n📦 Batch full ({BATCH_SIZE} records). Processing...")
                
                # Convert list of dictionaries into Pandas DataFrame
                df = pd.DataFrame(messages_batch)
                
                # Upload to S3 (this function handles conversion to Parquet and upload)
                upload_to_s3(df)
                
                # Commit offset manually! 
                # This tells Kafka that we have successfully processed these messages.
                # If an error occurs in upload_to_s3, the code crashes and commit doesn't happen.
                # This means when the consumer restarts, it will read those messages again (Zero Data Loss!)
                consumer.commit()
                
                # Clear the batch for the next round
                messages_batch.clear()
                print("-" * 50)
                
    except KeyboardInterrupt:
        print("\n🛑 Stopping Consumer...")
        consumer.close()
        print("✅ Consumer stopped safely.")
