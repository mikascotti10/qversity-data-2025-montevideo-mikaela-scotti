from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
import json
import psycopg2

def create_table_if_not_exists():
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze_mobile_customers (
            id SERIAL PRIMARY KEY,
            raw_json JSONB NOT NULL,
            ingestion_timestamp TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Tabla bronze_mobile_customers verificada/creada.")

def download_from_s3():
    bucket_name = 'qversity-raw-public-data'
    file_key = 'mobile_customers_messy_dataset.json'
    local_folder = '/opt/airflow/data/raw'
    os.makedirs(local_folder, exist_ok=True)
    local_path = os.path.join(local_folder, file_key)
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, file_key, local_path)
    print(f"Archivo descargado en: {local_path}")

def load_json_to_postgres():
    json_path = '/opt/airflow/data/raw/mobile_customers_messy_dataset.json'
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()
    with open(json_path, 'r') as f:
        data = json.load(f)
        for record in data:
            cur.execute(
                "INSERT INTO bronze_mobile_customers (raw_json) VALUES (%s)",
                [json.dumps(record)]
            )
    conn.commit()
    cur.close()
    conn.close()
    print("Datos insertados en la tabla bronze_mobile_customers.")

default_args = {
    'owner': 'qversity',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='bronze_ingest',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table_if_not_exists',
        python_callable=create_table_if_not_exists,
    )

    download_task = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_json_to_postgres',
        python_callable=load_json_to_postgres,
    )

    create_table_task >> download_task >> load_to_postgres_task
