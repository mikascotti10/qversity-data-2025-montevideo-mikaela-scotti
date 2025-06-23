from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG para correr solo los modelos Silver de dbt
with DAG(
    dag_id="silver_pipeline",
    schedule_interval=None,  # Ejecutálo manualmente o programalo si querés
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["silver", "dbt", "qversity"],
) as dag:

    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        bash_command="cd /dbt && dbt run --select silver*"
    )
