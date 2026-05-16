# Real-Time Stock Market Data Pipeline

Yeh project ek real-time data engineering pipeline hai. Isme hum live stock market data (jaise ki AAPL, TSLA ke stock prices) generate karenge aur usko AWS pe store karke analyze karenge. 

Yahan bilkul simple bhasha mein samjhaya gaya hai ki ye system kaise kaam karta hai, step-by-step.

---

## 🏗️ Project Architecture (Flow Kaise Kaam Karta Hai?)

Ek simple example socho: Amazon ya Flipkart par jab log har second orders place karte hain, toh wo data real-time mein system ke andar aana chahiye taaki analytics ho sake. Hum waisa hi ek system bana rahe hain, bas "Orders" ki jagah "Stock Prices" hain.

Yeh raha humara **End-to-End Flow**:

### 1. The Producer (Python Script)
- **Role:** Data Create Karna.
- **Kya karta hai:** Asal mein stock market ka live data lene ke liye paise lagte hain. Isliye hum ek `producer.py` script banayenge jo fake (lekin realistic) stock prices har second generate karegi. (e.g. `{"symbol": "AAPL", "price": 150.50, "timestamp": "2023-10-27..."}`).
- **Next Step:** Ye script is data ko apne paas nahi rakhti, seedha **Kafka** ko bhejti hai.

### 2. Apache Kafka (The Message Broker)
- **Role:** Data ko handle karna (Traffic Police).
- **Kya karta hai:** Socho agar 1 lakh log ek saath data bhej rahe hain, toh database crash ho jayega. Kafka ek "Buffer" ya waiting room ki tarah kaam karta hai. Producer data Kafka mein daal deta hai, aur Kafka usko safely store kar leta hai jab tak ki aage wala system usko lene ke liye ready na ho.
- **Important:** Ye tumhare Mac par locally chal raha hai.

### 3. The Consumer (Python Script)
- **Role:** Data ko read karna aur AWS par bhejna.
- **Kya karta hai:** Ye ek `consumer.py` script hogi. Ye lagataar Kafka se naya data maangti rehti hai. Jaise hi naya stock price aata hai, ye use read karti hai, 100-200 messages ka ek batch banati hai, aur phir use **AWS S3** par upload kar deti hai.

### 4. AWS S3 (Simple Storage Service)
- **Role:** Unlimited Storage (Data Lake).
- **Kya karta hai:** Ye AWS ka ek bada hard drive hai. Humara Consumer yahan par JSON ya Parquet format mein files save karega. Humne pehle wale project mein data local folder mein save kiya tha, yahan hum cloud (AWS) mein save kar rahe hain.

### 5. AWS Glue
- **Role:** Data ki Schema (Structure) samajhna.
- **Kya karta hai:** S3 mein bas files padi hain, AWS ko nahi pata ki un files ke andar columns kya hain (jaise ki 'price' ek number hai, 'symbol' ek text hai). AWS Glue ka **Crawler** un files ko padhta hai aur automatically ek table ka structure bana deta hai (isse "Data Catalog" kehte hain). Tumhe manually `CREATE TABLE` likhne ki zaroorat nahi padti!

### 6. AWS Athena
- **Role:** SQL Queries run karna.
- **Kya karta hai:** Ab jab Glue ne bata diya hai ki data ka structure kaisa hai, tum Athena mein jaakar SQL queries likh sakte ho (e.g., `SELECT AVG(price) FROM stock_data WHERE symbol = 'AAPL'`).
- **Speciality:** Athena "Serverless" hai. Iska matlab tumhe koi database setup nahi karna pada (jaise PostgreSQL kiya tha). Tum bas S3 files par seedha SQL likh rahe ho!

---

## 🛠️ Interview ke liye: AWS ka Kya Role Tha?

Agar koi pooche: *"Tumne AWS kyun use kiya is project mein?"*

Tumhara jawab:
> "Sir, Kafka ke through real-time data aa raha tha. Agar main isko traditional database mein daalta, toh storage limits aur scaling ka issue aata. Isliye maine **AWS S3** ko as a Data Lake use kiya kyunki wo infinitely scalable aur cheap hai. Phir data par SQL queries run karne ke liye main traditional DB nahi use karna chahta tha, isliye maine **AWS Glue** se schema infer karwaya aur **AWS Athena** se serverless SQL queries chalayi. Is stack ne pipeline ko scalable, fault-tolerant, aur cost-effective bana diya."

---

## 🚀 How to Run the Project Locally (Step-by-Step)

Jab bhi tum is project ki practice karna chaho ya interview se pehle revise karna chaho, ye steps follow karna:

### Step 1: Pre-requisites Ensure Karo
- Tumhare AWS credentials `02-stock-market-pipeline/.env` file mein set hone chahiye.
- Tumhara virtual environment activated hona chahiye.

Terminal kholo aur is folder mein jao:
```bash
cd ~/interview_prep/da-de-project/02-stock-market-pipeline
source venv/bin/activate
```

### Step 2: Start Apache Kafka (Background Service)
Agar Kafka band ho gaya hai, toh Mac par usko chalu karne ke liye ye command chalao:
```bash
brew services start kafka
```

### Step 3: Start the Real-Time Producer (Terminal 1)
Ek naya terminal tab kholo (Command + T) aur producer chalao:
```bash
cd ~/interview_prep/da-de-project/02-stock-market-pipeline
source venv/bin/activate
python producer.py
```
*Ye continuously fake data generate karke Kafka ko bhejta rahega.*

### Step 4: Start the S3 Consumer (Terminal 2) - Optional
Agar tum chahte ho ki data AWS S3 par save ho (Data Engineering flow), toh dusra terminal tab kholo:
```bash
cd ~/interview_prep/da-de-project/02-stock-market-pipeline
source venv/bin/activate
python consumer.py
```
*Ye batch banayega aur har 50 messages pe ek Parquet file S3 mein upload karega.*

### Step 5: Start the Real-Time Dashboard (Terminal 3) - Optional
Agar tumhe real-time <200ms latency wala UI check karna hai (Interview Demo), toh teesra terminal tab kholo:
```bash
cd ~/interview_prep/da-de-project/02-stock-market-pipeline
source venv/bin/activate
streamlit run dashboard.py
```
*Ye ek browser tab open karega (http://localhost:8501) jahan tumhe live moving charts dikhenge.*

### Step 6: Stop Everything
Kaam khatam hone ke baad terminals mein `Ctrl + C` dabao scripts band karne ke liye. Aur Kafka stop karne ke liye:
```bash
brew services stop kafka
```
