from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from utils_Bogdanov.check_generations_dag_functions import _start, _check_generations_count, _end_fail, _end_success

with DAG(dag_id="Bogdanov_Check_Generations_DAG",
         start_date=days_ago(2),
         schedule_interval="0 12 * * *",
         catchup=False,
         max_active_runs=1,
         tags=["Final Project"],
         ) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=_start
    )
    check_generations_amount = PythonOperator(
        task_id='check_generations_amount',
        python_callable=_check_generations_count,
        op_kwargs={
            "generations_api_catalog_url": "https://pokeapi.co/api/v2/generation"
        }
    )
    finish_success = PythonOperator(
        task_id='success',
        python_callable=_end_success
    )
    finish_fail = PythonOperator(
        task_id='fail',
        python_callable=_end_fail,
        trigger_rule='one_failed'
    )

    start >> check_generations_amount >> [finish_success, finish_fail]
