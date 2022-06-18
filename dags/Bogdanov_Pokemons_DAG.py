from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from utils_Bogdanov.dag_functions import _start, \
    _fetch_pokemons, _fetch_generations, _fetch_moves, _fetch_stats, _fetch_types,\
    _process_pokemons, _process_generations, _process_moves, _process_stats, _process_types, \
    _load_pokemons, _load_generations, _load_moves, _load_stats, _load_types, \
    _cleanup, _end_success, _end_fail


with DAG(dag_id="Bogdanov_Pokemons_Dag",
         start_date=days_ago(2),
         schedule_interval=None,
         catchup=False
         ) as dag:
    start = PythonOperator(
        task_id='start',
        python_callable=_start
    )
    # EXTRACT:
    fetch_pokemons = PythonOperator(
        task_id='fetch_pokemons',
        python_callable=_fetch_pokemons,
        op_kwargs={
            "api_catalog_url": "https://pokeapi.co/api/v2/pokemon/?limit=300",
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Pokemons/",
            "threads_count": 2,
            "requests_per_second": 5
        }
    )
    fetch_generations = PythonOperator(
        task_id='fetch_generations',
        python_callable=_fetch_generations,
        op_kwargs={
            "api_catalog_url": "https://pokeapi.co/api/v2/generation",
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Generations/",
            "threads_count": 2,
            "requests_per_second": 5
        }
    )
    fetch_moves = PythonOperator(
        task_id='fetch_moves',
        python_callable=_fetch_moves,
        op_kwargs={
            "api_catalog_url": "https://pokeapi.co/api/v2/move",
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Moves/",
            "threads_count": 2,
            "requests_per_second": 5
        }
    )
    fetch_stats = PythonOperator(
        task_id='fetch_stats',
        python_callable=_fetch_stats,
        op_kwargs={
            "api_catalog_url": "https://pokeapi.co/api/v2/stat",
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Stats/",
            "threads_count": 2,
            "requests_per_second": 5
        }
    )
    fetch_types = PythonOperator(
        task_id='fetch_types',
        python_callable=_fetch_types,
        op_kwargs={
            "api_catalog_url": "https://pokeapi.co/api/v2/type",
            "unprocessed_s3_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Types/",
            "threads_count": 2,
            "requests_per_second": 5
        }
    )
    # TRANSFORM:
    process_pokemons = PythonOperator(
        task_id='process_pokemons',
        python_callable=_process_pokemons,
        op_kwargs={
            "unprocessed_pokemons_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Pokemons/",
            "unprocessed_generations_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Generations/",
            "processed_pokemons_prefix": "{{ var.value.processed_files }}Bogdanov/Pokemons/"
        }
    )
    process_generations = PythonOperator(
        task_id='process_generations',
        python_callable=_process_generations,
        op_kwargs={
            "unprocessed_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Generations/",
            "processed_prefix": "{{ var.value.processed_files }}Bogdanov/Generations/",
        }
    )
    process_moves = PythonOperator(
        task_id='process_moves',
        python_callable=_process_moves,
        op_kwargs={
            "unprocessed_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Moves/",
            "processed_prefix": "{{ var.value.processed_files }}Bogdanov/Moves/",
        }
    )
    process_stats = PythonOperator(
        task_id='process_stats',
        python_callable=_process_stats,
        op_kwargs={
            "unprocessed_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Stats/",
            "processed_prefix": "{{ var.value.processed_files }}Bogdanov/Stats/",
        }
    )
    process_types = PythonOperator(
        task_id='process_types',
        python_callable=_process_types,
        op_kwargs={
            "unprocessed_prefix": "{{ var.value.unprocessed_files }}Bogdanov/Types/",
            "processed_prefix": "{{ var.value.processed_files }}Bogdanov/Types/",
        }
    )
    # LOAD
    load_pokemons = PythonOperator(
        task_id='load_pokemons',
        python_callable=_load_pokemons,
        op_kwargs={
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Pokemons/",
            "snowflake_s3_prefix": "{{ var.value.snowpipe_files }}Bogdanov/Pokemons/"
        }
    )
    load_generations = PythonOperator(
        task_id='load_generations',
        python_callable=_load_generations,
        op_kwargs={
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Generations/",
            "snowflake_s3_prefix": "{{ var.value.snowpipe_files }}Bogdanov/Generations/"
        }
    )
    load_moves = PythonOperator(
        task_id='load_moves',
        python_callable=_load_moves,
        op_kwargs={
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Moves/",
            "snowflake_s3_prefix": "{{ var.value.snowpipe_files }}Bogdanov/Moves/"
        }
    )
    load_stats = PythonOperator(
        task_id='load_stats',
        python_callable=_load_stats,
        op_kwargs={
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Stats/",
            "snowflake_s3_prefix": "{{ var.value.snowpipe_files }}Bogdanov/Stats/"
        }
    )
    load_types = PythonOperator(
        task_id='load_types',
        python_callable=_load_types,
        op_kwargs={
            "processed_s3_prefix": "{{ var.value.processed_files }}Bogdanov/Types/",
            "snowflake_s3_prefix": "{{ var.value.snowpipe_files }}Bogdanov/Types/"
        }
    )
    cleanup = PythonOperator(
        task_id='cleanup',
        python_callable=_cleanup,
        op_kwargs={
            "list_of_directories_to_clean": [
                "{{ var.value.unprocessed_files }}Bogdanov/Pokemons/",
                "{{ var.value.processed_files }}Bogdanov/Pokemons/",
                "{{ var.value.unprocessed_files }}Bogdanov/Generations/",
                "{{ var.value.processed_files }}Bogdanov/Generations/",
                "{{ var.value.unprocessed_files }}Bogdanov/Moves/",
                "{{ var.value.processed_files }}Bogdanov/Moves/",
                "{{ var.value.unprocessed_files }}Bogdanov/Stats/",
                "{{ var.value.processed_files }}Bogdanov/Stats/",
                "{{ var.value.unprocessed_files }}Bogdanov/Types/",
                "{{ var.value.processed_files }}Bogdanov/Types/"
            ]
        },
        trigger_rule='all_done'
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
    start >> [fetch_pokemons, fetch_generations, fetch_moves, fetch_stats, fetch_types]
    [fetch_pokemons, fetch_generations] >> process_pokemons >> load_pokemons
    fetch_generations >> process_generations >> load_generations
    fetch_moves >> process_moves >> load_moves
    fetch_stats >> process_stats >> load_stats
    fetch_types >> process_types >> load_types
    [load_pokemons, load_generations, load_moves, load_stats, load_types] >> cleanup
    cleanup >> [finish_success, finish_fail]

