import json
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

# Thiết lập thông số DAG
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 9, 3, 10, 00)
}

# Danh sách 10 mã coin cần lấy dữ liệu
SYMBOLS = ["BNBUSDT", "BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT",
           "LTCUSDT", "ETCUSDT", "PEPEUSDT", "DOGEUSDT", "ADAUSDT"]

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

# Tạo DAG chạy mỗi 5 phút
with DAG('crypto_streaming',
         default_args=default_args,
         schedule_interval='* * * * *',  # Chạy mỗi 5 phút
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_binance',
        python_callable=stream_data
    )