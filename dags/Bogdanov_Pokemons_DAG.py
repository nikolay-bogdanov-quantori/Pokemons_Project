from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from utils_Bogdanov.dag_functions import _start, _fetch_pokemons, _process_pokemons, _load_pokemons, _cleanup,\
    _end_success, _end_fail


with DAG(dag_id="Bogdanov_Pokemons_Dag",
         start_date=days_ago(2),
         schedule_interval=None,
         catchup=False
         ) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=_start
    )
    fetch_pokemons = PythonOperator(
        task_id='fetch_pokemons',
        python_callable=_fetch_pokemons,
        op_kwargs={
            "api_pokemons_catalog_url": "https://pokeapi.co/api/v2/pokemon/?limit=300",
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Pokemons/",
            "threads_count": 2,
            "requests_per_second": 5
        }
    )
    process_pokemons = PythonOperator(
        task_id='process_pokemons',
        python_callable=_process_pokemons,
        op_kwargs={
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Pokemons/",
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Pokemons/",
            "threads_count": 2,
            "pokemons_per_file": 100
        }
    )
    load_pokemons = PythonOperator(
        task_id='load_pokemons',
        python_callable=_load_pokemons,
        op_kwargs={
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Pokemons/",
            "snowflake_s3_prefix": "{{ var.value.snowpipe_files }}Bogdanov/Pokemons/"
        }
    )
    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=_cleanup,
        op_kwargs={
            "list_of_directories_to_clean": [
                "{{ var.value.unprocessed_files }}Bogdanov/Pokemons/",
                "{{ var.value.processed_files }}Bogdanov/Pokemons/"
            ]
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

    #start >> load_pokemons >> [finish_success, finish_fail]
    # start >> process_pokemons >> [finish_success, finish_fail]
    #start >> fetch_pokemons >> [finish_success, finish_fail]
    #start >> fetch_pokemons >> process_pokemons >> [finish_success, finish_fail]
    #start >> cleanup >> [finish_success, finish_fail]
    start >> fetch_pokemons >> process_pokemons >> load_pokemons >> cleanup >> [finish_success, finish_fail]
