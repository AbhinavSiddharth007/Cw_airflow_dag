import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Configuration ---
DATA_DIR = "/opt/airflow/data" 
POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "fuel_exports"

default_args = {
    'owner': 'daniel',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

def etl_parquet_to_postgres():
    search_path = os.path.join(DATA_DIR, "*.parquet")
    files = glob.glob(search_path)
    
    if not files:
        print("No new Parquet files found. Skipping.")
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for file_path in files:
        print(f"Processing file: {file_path}")
        df = pd.read_parquet(file_path)

        # 1. Flatten 'dock' struct
        if 'dock' in df.columns:
            dock_df = pd.json_normalize(df['dock'])
            df['dock_bay'] = dock_df['bay']
            df['dock_level'] = dock_df['level']
            df.drop(columns=['dock'], inplace=True)

        # 2. Convert 'services' array/list to string
        if 'services' in df.columns:
            # Combined check for both Python lists and NumPy arrays
            df['services'] = df['services'].apply(
                lambda x: ", ".join(x) if isinstance(x, (list, np.ndarray)) else x
            )

        # 3. Load to Postgres
        try:
            df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
            os.remove(file_path)
            print(f"Successfully uploaded and removed {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise # Re-raise so Airflow marks the task as failed

# --- DAG Definition ---
with DAG(
    dag_id='fuel_exports_etl_v1',
    default_args=default_args,
    description='ETL for Parquet fuel transactions',
    schedule_interval=timedelta(minutes=1), # Runs every minute
    catchup=False,
    tags=['data_science', 'etl']
) as dag:

    run_etl = PythonOperator(
        task_id='process_parquet_files',
        python_callable=etl_parquet_to_postgres
    )

    run_etl
