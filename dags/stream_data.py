import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer, KafkaConsumer
import psycopg2
import logging

# Thiết lập thông số DAG
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# Danh sách 10 mã coin cần lấy dữ liệu
SYMBOLS = ["BNBUSDT", "1INCHUSDT", "AXSUSDT", "ENJUSDT", "XLMUSDT"]

# Hàm lấy dữ liệu từ Binance API
def get_data(symbol):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=5m&limit=1"
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        return []

# Hàm format dữ liệu theo yêu cầu
def format_data(symbol, data):
    return {
        "symbol": symbol,
        "open": float(data[1]),
        "high": float(data[2]),
        "low": float(data[3]),
        "close": float(data[4]),
        "volume": float(data[5]),
        "time": int(data[0])
    }

# Hàm streaming dữ liệu vào Kafka
def stream_data():
    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for symbol in SYMBOLS:
        try:
            data = get_data(symbol)
            if data:
                formatted_data = format_data(symbol, data[0])  # Lấy cây nến gần nhất
                producer.send('crypto_kline', formatted_data)
                print(f"Sent to Kafka: {formatted_data}")
        except Exception as e:
            print(f"Error streaming data for {symbol}: {e}")

def consume_from_kafka_and_store():
    records = 0

    consumer = KafkaConsumer(
        'crypto_kline',
        bootstrap_servers=['broker:29092'],
        group_id='airflow_batch_consumer',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=15000  # Dừng nếu quá 15s không có dữ liệu
    )

    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_data (
            symbol TEXT NOT NULL,
            time TIMESTAMP NOT NULL,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume FLOAT,
            PRIMARY KEY (symbol, time)
        );
    """)
    conn.commit()

    for msg in consumer:
        d = msg.value
        print(f"📥 {records+1} - Received: {d}")

        cursor.execute("""
            INSERT INTO crypto_data (symbol, time, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, time) DO NOTHING;
        """, (
            d["symbol"],datetime.fromtimestamp(d["time"] / 1000),
            d["open"], d["high"], d["low"], d["close"], d["volume"]
        ))

        records += 1

    conn.commit()
    cursor.close()
    conn.close()
    consumer.close()

    print(f"✅ Inserted {records} records into PostgreSQL and exited.")

# Tạo DAG chạy mỗi 5 phút
with DAG('crypto_streaming',
         default_args=default_args,
         schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_binance',
        python_callable=stream_data
    )
    consume_from_kafka = PythonOperator(
        task_id='consume_kafka_to_postgres',
        python_callable=consume_from_kafka_and_store
    )

    streaming_task >> consume_from_kafka