from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from joblib import load, dump
from xgboost import XGBRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
from scipy.stats import skew, kurtosis, entropy
from scipy.fft import fft, fftfreq
import pywt
from statsmodels.tsa.stattools import adfuller, acf, pacf
import nolds
import os

# =============== CONFIG ===============
POSTGRES_USER = 'airflow'
POSTGRES_PW = 'airflow'
POSTGRES_HOST = 'postgres'
POSTGRES_PORT = '5432'
POSTGRES_DB = 'airflow'
TABLE_NAME = 'crypto_data'
PREDICT_TABLE = 'predictions'
SYMBOLS = ["BNBUSDT", "1INCHUSDT", "AXSUSDT", "ENJUSDT", "XLMUSDT"]
WINDOW_SIZE = 64
N_PER_DAY = 6  # Số bản ghi 4h mỗi ngày
model_dir = "/opt/airflow/models"
os.makedirs(model_dir, exist_ok=True)

# =============== FUNCTIONS ===============
def read_latest_data_from_postgres(symbol, days_back=30):
    url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PW}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'
    engine = create_engine(url)
    query = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE symbol = '{symbol}'
          AND time >= NOW() - INTERVAL '{days_back} days'
        ORDER BY time ASC
    """
    df = pd.read_sql(query, engine)
    engine.dispose()
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time')
    # Resample 4h
    df_4h = df.resample('4H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }).dropna().reset_index()
    df_4h['symbol'] = symbol
    return df_4h

def remove_outlier_iqr(X, y, feature_cols=None):
    mask = np.ones(len(y), dtype=bool)
    Q1 = np.percentile(y, 25)
    Q3 = np.percentile(y, 75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    mask &= (y >= lower_bound) & (y <= upper_bound)
    if feature_cols is not None:
        for col in feature_cols:
            col_values = X[col].values
            Q1 = np.percentile(col_values, 25)
            Q3 = np.percentile(col_values, 75)
            IQR = Q3 - Q1
            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR
            mask &= (col_values >= lower) & (col_values <= upper)
    return X[mask].reset_index(drop=True), y[mask]

def hjorth_params(signal):
    first_deriv = np.diff(signal)
    second_deriv = np.diff(first_deriv)
    var_zero = np.var(signal)
    var_d1 = np.var(first_deriv)
    var_d2 = np.var(second_deriv)
    mobility = np.sqrt(var_d1 / var_zero)
    complexity = np.sqrt(var_d2 / var_d1) / mobility
    return mobility, complexity

def extract_super_features(series, fft_sample_size=128, wavelet='db4', wavelet_level=3):
    features = {}
    features['mean'] = np.mean(series)
    features['std'] = np.std(series)
    features['skewness'] = skew(series)
    features['kurtosis'] = kurtosis(series)
    features['median'] = np.median(series)
    features['iqr'] = np.percentile(series, 75) - np.percentile(series, 25)
    features['entropy'] = entropy(np.histogram(series, bins=10, density=True)[0] + 1e-10)
    features['min'] = np.min(series)
    features['max'] = np.max(series)
    features['range'] = np.max(series) - np.min(series)
    features['mad'] = np.mean(np.abs(series - np.mean(series)))
    features['ptp'] = np.ptp(series)
    features['energy'] = np.sum(series ** 2)
    features['zero_crossings'] = ((series[:-1] * series[1:]) < 0).sum()
    returns = np.diff(series) / series[:-1]
    features['realized_volatility'] = np.sqrt(np.sum(returns**2))
    features['std_return'] = np.std(returns)
    features['mean_return'] = np.mean(returns)
    features['skew_return'] = skew(returns)
    features['kurt_return'] = kurtosis(returns)
    features['acf_return_1'] = acf(returns, nlags=1)[1]
    roll_window = min(len(series), 10)
    features['rolling_mean'] = np.mean(series[-roll_window:])
    features['rolling_std'] = np.std(series[-roll_window:])
    features['rolling_min'] = np.min(series[-roll_window:])
    features['rolling_max'] = np.max(series[-roll_window:])
    ema = pd.Series(series).ewm(span=10).mean().values
    features['ema10'] = ema[-1]
    features['ema_diff'] = series[-1] - ema[-1]
    rolling_mean = pd.Series(series).rolling(window=10).mean().values
    rolling_std = pd.Series(series).rolling(window=10).std().values
    features['boll_upper'] = rolling_mean[-1] + 2 * rolling_std[-1]
    features['boll_lower'] = rolling_mean[-1] - 2 * rolling_std[-1]
    series_fft = series[-fft_sample_size:]
    N = len(series_fft)
    yf = fft(series_fft)
    xf = fftfreq(N, d=1)[:N//2]
    magnitude = 2.0 / N * np.abs(yf[:N//2])
    features['fft_dominant_freq'] = xf[np.argmax(magnitude)]
    features['fft_spectral_energy'] = np.sum(magnitude ** 2)
    spectral_prob = magnitude / np.sum(magnitude)
    features['fft_spectral_entropy'] = -np.sum(spectral_prob * np.log(spectral_prob + 1e-10))
    coeffs = pywt.wavedec(series, wavelet, level=wavelet_level)
    for i, coeff in enumerate(coeffs):
        features[f'wavelet_L{i}_mean'] = np.mean(coeff)
        features[f'wavelet_L{i}_std'] = np.std(coeff)
        features[f'wavelet_L{i}_energy'] = np.sum(coeff ** 2)
    adf_stat, adf_pvalue, *_ = adfuller(series)
    features['adf_stat'] = adf_stat
    features['adf_pvalue'] = adf_pvalue
    acf_vals = acf(series, nlags=10)
    pacf_vals = pacf(series, nlags=10)
    for i in range(1, 6):
        features[f'acf_lag_{i}'] = acf_vals[i]
        features[f'pacf_lag_{i}'] = pacf_vals[i]
    features['hurst_exp'] = nolds.hurst_rs(series)
    features['dfa'] = nolds.dfa(series)
    features['corr_dim'] = nolds.corr_dim(series, emb_dim=3)
    mobility, complexity = hjorth_params(series)
    features['hjorth_mobility'] = mobility
    features['hjorth_complexity'] = complexity
    return features

# =============== TRAIN TASK ===============
def retrain_models():
    from sklearn.metrics import mean_absolute_error
    for symbol in SYMBOLS:
        print(f"\nRetrain model cho {symbol}")
        df = read_latest_data_from_postgres(symbol, days_back=30)
        if df.shape[0] < WINDOW_SIZE+20:
            print(f"Không đủ dữ liệu để train cho {symbol}, bỏ qua.")
            continue
        df = df.sort_values('time').reset_index(drop=True)
        def extract_feature_target(i):
            series = df['close'].values[i-WINDOW_SIZE:i].copy()
            features = extract_super_features(series)
            target = df['close'].values[i+1]
            return features, target
        results = [extract_feature_target(i) for i in range(WINDOW_SIZE, len(df)-1)]
        feature_rows, label_rows = zip(*results)
        X = pd.DataFrame(feature_rows)
        y = np.array(label_rows)
        X, y = remove_outlier_iqr(X, y)
        if len(y) < 30:
            print(f"Không đủ dữ liệu sau loại outlier cho {symbol}, bỏ qua.")
            continue
        split_idx = int(len(y) * 0.8)
        X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
        y_train, y_test = y[:split_idx], y[split_idx:]
        scaler_y = MinMaxScaler()
        y_train_scaled = scaler_y.fit_transform(y_train.reshape(-1, 1)).flatten()
        model_xgb = XGBRegressor(n_estimators=100, learning_rate=0.05, max_depth=6,
                                 subsample=0.8, colsample_bytree=0.8,
                                 objective='reg:squarederror', random_state=42)
        model_xgb.fit(X_train.values, y_train_scaled)
        model_rf = RandomForestRegressor(n_estimators=100, max_depth=6, random_state=42, n_jobs=-1)
        model_rf.fit(X_train.values, y_train_scaled)
        y_pred_xgb = scaler_y.inverse_transform(model_xgb.predict(X_test.values).reshape(-1, 1)).flatten()
        y_pred_rf = scaler_y.inverse_transform(model_rf.predict(X_test.values).reshape(-1, 1)).flatten()
        mae_xgb = mean_absolute_error(y_test, y_pred_xgb)
        mae_rf = mean_absolute_error(y_test, y_pred_rf)
        if mae_xgb <= mae_rf:
            best_name = 'xgb'
        else:
            best_name = 'rf'
        dump(model_xgb, os.path.join(model_dir, f'{symbol}_xgb_model.joblib'))
        dump(model_rf, os.path.join(model_dir, f'{symbol}_rf_model.joblib'))
        dump(scaler_y, os.path.join(model_dir, f'{symbol}_scaler_y.joblib'))
        X.columns.to_series().to_csv(os.path.join(model_dir, f'{symbol}_feature_names.csv'), index=False)
        with open(os.path.join(model_dir, f'{symbol}_best_model.txt'), 'w') as f:
            f.write(best_name)
        print(f"Lưu model cho {symbol} - best: {best_name}")


# =============== DAG DEFINITION ===============
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    'retrain_and_predict_crypto_models',
    default_args=default_args,
    schedule_interval=timedelta(days=30),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'crypto', 'postgres'],
) as dag:

    retrain_task = PythonOperator(
        task_id='retrain_models',
        python_callable=retrain_models,
    )

    retrain_task