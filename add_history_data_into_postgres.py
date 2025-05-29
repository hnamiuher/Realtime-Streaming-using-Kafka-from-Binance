import psycopg2
import pandas as pd
import logging

# Thiết lập logging
logging.basicConfig(level=logging.INFO)

def connect_to_postgres():
    """
    Kết nối đến PostgreSQL.
    """
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="localhost",  # Hoặc "postgres" nếu chạy trong Docker Compose
            port="5432"
        )
        logging.info("✅ Connected to PostgreSQL.")
        return conn
    except Exception as e:
        logging.error(f"❌ Failed to connect to PostgreSQL: {e}")
        return None

def insert_data_into_postgres(conn, df):
    """
    Chèn dữ liệu từ DataFrame vào PostgreSQL.
    
    :param conn: Kết nối PostgreSQL
    :param df: DataFrame chứa dữ liệu lịch sử
    """
    try:
        cursor = conn.cursor()

        # Ghi dữ liệu với UPSERT
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO crypto_data (symbol, time, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, time) DO NOTHING;  -- Không ghi đè nếu đã tồn tại
            """, (row['symbol'], row['time'], row['open'], row['high'], row['low'], row['close'], row['volume']))

        conn.commit()
        cursor.close()
        logging.info(f"✅ Inserted {len(df)} rows into PostgreSQL.")
    except Exception as e:
        logging.error(f"❌ Failed to insert data into PostgreSQL: {e}")

if __name__ == "__main__":
    # Đường dẫn đến file CSV chứa dữ liệu lịch sử
    csv_file = r"C:\test_khoaluan\Realtime-Streaming-using-Kafka-from-Binance\crypto_history_5m_last_1_year.csv"

    # Đọc dữ liệu từ file CSV
    try:
        df = pd.read_csv(csv_file)
        logging.info(f"✅ Loaded {len(df)} rows from {csv_file}.")
    except Exception as e:
        logging.error(f"❌ Failed to load CSV file: {e}")
        exit()

    # Kết nối đến PostgreSQL
    conn = connect_to_postgres()
    if conn:
        # Chèn dữ liệu vào PostgreSQL
        insert_data_into_postgres(conn, df)
        conn.close()    