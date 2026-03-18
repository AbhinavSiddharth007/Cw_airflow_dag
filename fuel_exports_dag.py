import glob
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Configuration ---
POSTGRES_CONN_ID = "postgres_default"
DATA_DIR = "/opt/airflow/data"
TABLE_NAME = "fuel_exports"

default_args = {
    'retries': 1,
    'owner': 'daniel',
    'start_date': datetime(2026, 3, 1),
    'depends_on_past': False,
    'retry_delay': timedelta(seconds=30),
}


def _flatten_dock_column(df):
    """Expand the nested 'dock' struct into individual bay and level columns."""
    if 'dock' not in df.columns:
        return df
    dock_df = pd.json_normalize(df['dock'])
    df['dock_bay'] = dock_df['bay']
    df['dock_level'] = dock_df['level']
    return df.drop(columns=['dock'])


def _stringify_services(df):
    """Convert 'services' list/array values into comma-separated strings."""
    if 'services' not in df.columns:
        return df
    df['services'] = df['services'].apply(
        lambda x: ", ".join(x) if isinstance(x, (list, np.ndarray)) else x
    )
    return df


def _load_file(file_path, engine):
    """Read, transform, and push a single parquet file into Postgres."""
    print(f"Processing file: {file_path}")
    df = pd.read_parquet(file_path)

    df = _flatten_dock_column(df)
    df = _stringify_services(df)

    df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    os.remove(file_path)
    print(f"Successfully uploaded and removed {file_path}")


def etl_parquet_to_postgres():
    search_path = os.path.join(DATA_DIR, "*.parquet")
    files = glob.glob(search_path)

    if not files:
        print("No new Parquet files found. Skipping.")
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for file_path in files:
        try:
            _load_file(file_path, engine)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            raise  # Re-raise so Airflow marks the task as failed


# --- DAG Definition ---
with DAG(
    dag_id='fuel_exports_etl_v1',
    default_args=default_args,
    catchup=False,
    tags=['data_science', 'etl'],
    schedule_interval=timedelta(minutes=1),  # Runs every minute
    description='ETL for Parquet fuel transactions',
) as dag:

    run_etl = PythonOperator(
        task_id='process_parquet_files',
        python_callable=etl_parquet_to_postgres,
    )

    run_etl
