from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from sqlalchemy import create_engine
import pandas as pd
from joblib import load
from train_30days import read_latest_data_from_postgres, extract_super_features

POSTGRES_USER = 'airflow'
POSTGRES_PW = 'airflow'
POSTGRES_HOST = 'postgres'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'airflow'
TABLE_NAME = 'crypto_data'
PREDICT_TABLE = 'predictions'
model_dir = "/opt/airflow/models"
SYMBOLS = ["BNBUSDT", "1INCHUSDT", "AXSUSDT", "ENJUSDT", "XLMUSDT"]
WINDOW_SIZE = 64

def upsert_prediction(pred_row, engine):
    from sqlalchemy import text
    for _, row in pred_row.iterrows():
        query = text(f"""
        INSERT INTO {PREDICT_TABLE} (symbol, predict_time, predict_price, model_type, predict_at)
        VALUES (:symbol, :predict_time, :predict_price, :model_type, :predict_at)
        ON CONFLICT (symbol, predict_time) 
        DO UPDATE SET
            predict_price = EXCLUDED.predict_price,
            model_type = EXCLUDED.model_type,
            predict_at = EXCLUDED.predict_at
        """)
        with engine.begin() as conn:
            conn.execute(query, **row.to_dict())

def predict_next_4h_trailing(**context):
    # Lấy logical_date (chuẩn airflow >=2.2), fallback nếu cần
    logical_date = context.get('logical_date', None)
    if logical_date is None:
        logical_date = context.get('execution_date', datetime.utcnow())
    execution_time = pd.to_datetime(str(logical_date)).replace(tzinfo=None, second=0, microsecond=0)
    pred_time = execution_time + pd.Timedelta(hours=4)  # Dự báo cho 4 tiếng tiếp theo

    preds = []
    for symbol in SYMBOLS:
        try:
            with open(os.path.join(model_dir, f'{symbol}_best_model.txt'), 'r') as f:
                best_name = f.read().strip()
            model_path = os.path.join(model_dir, f'{symbol}_{best_name}_model.joblib')
            scaler_path = os.path.join(model_dir, f'{symbol}_scaler_y.joblib')
            feat_path = os.path.join(model_dir, f'{symbol}_feature_names.csv')
            model = load(model_path)
            scaler_y = load(scaler_path)
            feature_names = pd.read_csv(feat_path).values.ravel()
        except Exception as e:
            print(f"Không tìm thấy model cho {symbol}: {e}")
            continue
        day_back = 30
        df_predict = read_latest_data_from_postgres(symbol, days_back=day_back)
        df_predict = df_predict.sort_values('time').reset_index(drop=True)
        df_predict['time'] = pd.to_datetime(df_predict['time']).dt.tz_localize(None)
        history = df_predict[df_predict['time'] < execution_time]
        if len(history) < WINDOW_SIZE:
            print(f"Không đủ data predict cho {symbol}")
            continue
        close_series = history['close'].values[-WINDOW_SIZE:]
        features = extract_super_features(close_series)
        X_pred = pd.DataFrame([features])[feature_names]
        y_pred_scaled = model.predict(X_pred.values)
        y_pred = scaler_y.inverse_transform(y_pred_scaled.reshape(-1, 1)).flatten()[0]
        preds.append({
            'symbol': symbol,
            'predict_time': pred_time,
            'predict_price': float(y_pred),
            'model_type': best_name,
            'predict_at': pd.Timestamp.now()
        })
        print(f'Predicted {symbol} for {pred_time}: {y_pred:.2f} by {best_name}')
    if preds:
        url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
        engine = create_engine(url)
        pred_row = pd.DataFrame(preds)
        upsert_prediction(pred_row, engine)
        engine.dispose()

default_args = {
    'owner': 'nam hoai hub',
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    'predict_next_price_4h_rolling',
    default_args=default_args,
    schedule_interval='*/5 * * * *',  # Chạy mỗi 5 phút
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['crypto', 'postgres'],
) as dag:
    predict_task = PythonOperator(
        task_id='predict_next_4h_trailing',
        python_callable=predict_next_4h_trailing,
        provide_context=True
    )
