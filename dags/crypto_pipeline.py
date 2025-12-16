from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_market_data_pipeline',
    default_args=default_args,
    description='Full ELT: Extract -> Load -> Transform (dbt)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['crypto', 'elt'],
) as dag:

    # Extract
    extract_data = BashOperator(
        task_id='extract_crypto_data',
        bash_command='python /opt/airflow/scripts/extract.py',
    )

    # Load
    load_data = BashOperator(
        task_id='load_crypto_data',
        bash_command='python /opt/airflow/scripts/load.py',
    )

    # Transform (using dbt)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='pip install dbt-postgres && cd /opt/airflow/dbt && dbt run --profiles-dir .',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='pip install dbt-postgres && cd /opt/airflow/dbt && dbt test --profiles-dir .',
    )

    # Defining dependencies
    extract_data >> load_data >> dbt_run >> dbt_test