from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import random
from sqlalchemy import create_engine

# ==========================================
# 1. TAHAP EXTRACT & TRANSFORM (MOCK DATA)
# ==========================================
def run_etl_process():
    print("Mulai mengekstrak data keuangan publik...")
    
    # Karena container Docker tidak ada akses internet eksternal,
    # kita simulasikan penarikan data Kurs & JIBOR secara lokal.
    time_updated = datetime.now()
    
    # Simulasi fluktuasi angka keuangan OJK/BI
    rate_usd = random.uniform(15000, 16000)
    rate_jibor = random.uniform(0.05, 0.07)
    
    # Membuat DataFrame menggunakan Pandas
    df = pd.DataFrame([{
        'tanggal_update': time_updated,
        'kurs_usd': float(rate_usd),
        'jibor_rate': float(rate_jibor),
        'dibuat_pada': datetime.now()
    }])
    
    print("Data berhasil diekstrak dan dibersihkan menggunakan Pandas:")
    print(df)
    
    # ==========================================
    # 2. TAHAP LOAD (MEMASUKKAN KE DATABASE POSTGRES)
    # ==========================================
    print("Menghubungkan ke database PostgreSQL dan menyimpan data...")
    engine = create_engine('postgresql+psycopg2://airflow:airflow_password@postgres:5432/finance_db')
    
    # Menyimpan ke dalam tabel bernama 'bi_financial_rates'
    df.to_sql('bi_financial_rates', engine, if_exists='append', index=False)
    print("Data sukses disimpan ke database PostgreSQL!")

# ==========================================
# 3. MENGATUR JADWAL & ORKESTRASI (AIRFLOW DAG)
# ==========================================
default_args = {
    'owner': 'Irpan Rambe',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'bi_jibor_financial_pipeline',
    default_args=default_args,
    description='Pipeline otomatis untuk mengunduh data keuangan Bank Indonesia',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    task_jalankan_etl = PythonOperator(
        task_id='eksekusi_etl_keuangan',
        python_callable=run_etl_process
    )