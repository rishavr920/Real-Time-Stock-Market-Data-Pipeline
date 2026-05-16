# pyrefly: ignore [missing-import]
import streamlit as st
import pandas as pd
import json
import time
# pyrefly: ignore [missing-import]
from kafka import KafkaConsumer

# ==========================================
# STREAMLIT PAGE SETUP
# ==========================================
st.set_page_config(
    page_title="Real-Time Stock Dashboard",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Real-Time Stock Market Dashboard")
st.markdown("Consuming live events from **Apache Kafka** with `<200ms` latency.")

# Data ko memory mein store karne ke liye dataframe
if 'stock_data' not in st.session_state:
    st.session_state.stock_data = pd.DataFrame()

# ==========================================
# KAFKA CONSUMER SETUP
# ==========================================
# @st.cache_resource ensure karta hai ki consumer baar-baar restart na ho
@st.cache_resource
def get_kafka_consumer():
    try:
        return KafkaConsumer(
            'stock-events',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest', # Sirf naye live messages chahiye
            group_id='streamlit-dashboard'
        )
    except Exception as e:
        st.error(f"Kafka connection error: {e}. Kya Kafka chal raha hai?")
        return None

consumer = get_kafka_consumer()

# ==========================================
# UI LAYOUT
# ==========================================
st.markdown("### Live Prices")
metrics_placeholder = st.empty()

st.markdown("### Price Trends")
chart_placeholder = st.empty()

st.sidebar.markdown("### Controls")
st.sidebar.info("💡 Tip: `producer.py` ko terminal mein chalake rakho tabhi yahan live data dikhega!")
stop_button = st.sidebar.button("Stop Live Feed")

if not consumer:
    st.stop()

# ==========================================
# REAL-TIME UPDATE LOOP
# ==========================================
latest_prices = {}
status_text = st.sidebar.empty()

while not stop_button:
    status_text.text("Status: 🟢 Listening for events...")
    
    # Kafka se data padho (0.5 sec ka timeout)
    msg_pack = consumer.poll(timeout_ms=500)
    
    if msg_pack:
        new_records = []
        for tp, messages in msg_pack.items():
            for message in messages:
                event = message.value
                symbol = event['symbol']
                price = event['price']
                timestamp = pd.to_datetime(event['event_time'])
                
                # Metrics ke liye latest price update karo
                latest_prices[symbol] = price
                
                new_records.append({
                    'timestamp': timestamp,
                    symbol: price
                })
        
        if new_records:
            # Naye data ko dataframe mein dalo
            df_new = pd.DataFrame(new_records)
            df_new.set_index('timestamp', inplace=True)
            
            # Puraane data ke saath jod do
            st.session_state.stock_data = pd.concat([st.session_state.stock_data, df_new])
            
            # Browser freeze na ho isliye sirf aakhri 100 entries rakho
            if len(st.session_state.stock_data) > 100:
                st.session_state.stock_data = st.session_state.stock_data.iloc[-100:]
                
            # Missing data ko forward fill karo taaki chart pe lines toote nahi
            plot_data = st.session_state.stock_data.ffill().bfill()
            
            # 1. Chart update karo
            with chart_placeholder.container():
                st.line_chart(plot_data)
                
            # 2. Top 5 Metrics update karo
            with metrics_placeholder.container():
                cols = st.columns(5)
                symbols = list(latest_prices.keys())[:5]
                for i, sym in enumerate(symbols):
                    cols[i].metric(label=sym, value=f"${latest_prices[sym]:.2f}")
                    
    time.sleep(0.1) # CPU bachaane ke liye chhota sa pause
