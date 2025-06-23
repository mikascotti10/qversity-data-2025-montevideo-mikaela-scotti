
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG para correr todos los modelos ubicados en la carpeta /models/gold
with DAG(
    dag_id="gold_pipeline",
    schedule_interval=None,  # Ejecutálo manualmente o programalo si querés
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "dbt", "qversity"],
) as dag:

    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command="cd /dbt && dbt run --select path:models/gold"
    )
