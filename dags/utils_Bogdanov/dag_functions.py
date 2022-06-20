import os
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import List, Any
from utils_Bogdanov.fetching_funcs import _fetch_api_url_async, \
    load_string_on_s3, split_list_into_chunks_by_size_of_partition

from utils_Bogdanov.pokemon import\
    Pokemon, PokemonApiParserJson, PokemonUnprocessedEncoder, PokemonProcessedEncoder, json_to_pokemon

from utils_Bogdanov.generation import \
    Generation, GenerationApiParserJson, GenerationUnprocessedEncoder, GenerationProcessedEncoder, json_to_generation

from utils_Bogdanov.move import \
    Move, MoveApiParserJson, MoveUnprocessedEncoder, MoveProcessedEncoder, json_to_move

from utils_Bogdanov.stat import \
    Stat, StatApiParserJson, StatUnprocessedEncoder, StatProcessedEncoder, json_to_stat

from utils_Bogdanov.type import \
    Type, TypeApiParserJson, TypeUnprocessedEncoder, TypeProcessedEncoder, json_to_type


def read_json_from_s3(s3hook: S3Hook, bucket: str, key: str) -> Any:
    return json.loads(s3hook.read_key(key=key, bucket_name=bucket))


def get_filenames_from_s3(s3hook: S3Hook, bucket: str, key: str) -> List[str]:
    return [key for key in s3hook.list_keys(bucket_name=bucket, prefix=key) if os.path.split(key)[-1]]


def _start() -> None:
    print("Dag started")


def _end_success() -> None:
    print('Dag has finished successfully!')


def _end_fail() -> None:
    print('Dag execution failed!')


def _fetch_pokemons(api_catalog_url: str, unprocessed_s3_prefix: str, threads_count: int, requests_per_second: int):
    _fetch_api_url_async(
        api_catalog_url,
        unprocessed_s3_prefix,
        threads_count,
        requests_per_second,
        PokemonApiParserJson,
        PokemonUnprocessedEncoder)


def _fetch_generations(api_catalog_url: str, unprocessed_s3_prefix: str, threads_count: int, requests_per_second: int):
    _fetch_api_url_async(
        api_catalog_url,
        unprocessed_s3_prefix,
        threads_count,
        requests_per_second,
        GenerationApiParserJson,
        GenerationUnprocessedEncoder)


def _fetch_moves(api_catalog_url: str, unprocessed_s3_prefix: str, threads_count: int, requests_per_second: int):
    _fetch_api_url_async(
        api_catalog_url,
        unprocessed_s3_prefix,
        threads_count,
        requests_per_second,
        MoveApiParserJson,
        MoveUnprocessedEncoder)


def _fetch_stats(api_catalog_url: str, unprocessed_s3_prefix: str, threads_count: int, requests_per_second: int):
    _fetch_api_url_async(
        api_catalog_url,
        unprocessed_s3_prefix,
        threads_count,
        requests_per_second,
        StatApiParserJson,
        StatUnprocessedEncoder)


def _fetch_types(api_catalog_url: str, unprocessed_s3_prefix: str, threads_count: int, requests_per_second: int):
    _fetch_api_url_async(
        api_catalog_url,
        unprocessed_s3_prefix,
        threads_count,
        requests_per_second,
        TypeApiParserJson,
        TypeUnprocessedEncoder)


def _process_pokemons(unprocessed_pokemons_prefix: str,
                      unprocessed_generations_prefix: str,
                      processed_pokemons_prefix: str,
                      objects_per_file: int):
    """
    для процессинга покемонов я не использовал общую для всех _process.* тасок, функцию combine_files_into_one_and_save_on_s3
    так как хотел избавиться от необходимости тянуть в базу pokemon_species,
    вместо этого я вычисляю generation покемона ниже, пользуясь тем, что отношение
    species к generations содержит довольно мало строк
    """
    s3hook = S3Hook()
    unprocessed_pokemons_bucket, unprocessed_pokemons_key = S3Hook.parse_s3_url(unprocessed_pokemons_prefix)
    unprocessed_generations_bucket, unprocessed_generations_key = S3Hook.parse_s3_url(unprocessed_generations_prefix)
    #
    pokemons_filenames = get_filenames_from_s3(s3hook, unprocessed_pokemons_bucket, unprocessed_pokemons_key)
    generations_filenames = get_filenames_from_s3(s3hook, unprocessed_generations_bucket, unprocessed_generations_key)
    #
    species_to_generations: dict[str, int] = {}  # позволяет вычислить generation, зная pokemon.species
    for generation_filename in generations_filenames:
        print(f"reading {generation_filename}")
        json_string = read_json_from_s3(s3hook, unprocessed_generations_bucket, generation_filename)
        generation: Generation = json_to_generation(json_string)
        # каждое species относится только к одному поколению, так что использование словаря возможно
        for species in generation.species:
            if species_to_generations.get(species) is not None:  # но на всякий случай проверим, не перезаписываем ли мы данные
                raise ValueError(
                    f"Error! species {species} relates to more than one generation: [{species_to_generations.get(species)}, {generation.id}]"
                )
            species_to_generations[species] = generation.id
    #
    pokemons_partitioned_filenames = split_list_into_chunks_by_size_of_partition(pokemons_filenames, objects_per_file)
    for q, filenames_part in enumerate(pokemons_partitioned_filenames):
        pokemons_list: list[Pokemon] = [None]*len(filenames_part)
        for j, pokemon_filename in enumerate(filenames_part):
            print(f"reading {pokemon_filename}")
            json_string = read_json_from_s3(s3hook, unprocessed_pokemons_bucket, pokemon_filename)
            pokemons_list[j] = json_to_pokemon(json_string)
            pokemons_list[j].generation = species_to_generations[pokemons_list[j].species]
        filename = os.path.join(processed_pokemons_prefix, f"processed_pokemons_part_{q+1}.json")
        json_string = json.dumps(pokemons_list, cls=PokemonProcessedEncoder, indent=4)
        load_string_on_s3(data=json_string, key=filename)


def combine_files_into_one_and_save_on_s3(
        s3hook,
        source_s3_bucket: str,
        all_filenames: List[str],
        objects_per_file: int,
        object_read_function,
        processed_encoder_class: json.JSONEncoder,
        result_files_prefix: str,
        target_s3_prefix: str
) -> None:
    """
    Функция последовательно обрабатывает партиции имен файлов, извлекая из этих файлов объекты, определяемые
    object_read_function, создает списки объектов длиной objects_per_file и записывает комбинированный файл на s3
    c заданным префиксом
    """
    partitioned_filenames = split_list_into_chunks_by_size_of_partition(all_filenames, objects_per_file)
    for q, filenames_part in enumerate(partitioned_filenames):
        objects_list: list[Any] = [None]*len(filenames_part)
        for j, filename in enumerate(filenames_part):
            print(f"reading {filename}")
            json_string = read_json_from_s3(s3hook, source_s3_bucket, filename)
            objects_list[j] = object_read_function(json_string)
        result_filename = os.path.join(target_s3_prefix, f"{result_files_prefix}_part_{q+1}.json")
        combined_json_string = json.dumps(objects_list, cls=processed_encoder_class, indent=4)
        load_string_on_s3(data=combined_json_string, key=result_filename)


def _process_generations(unprocessed_prefix: str, processed_prefix: str, objects_per_file: int):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    generations_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    combine_files_into_one_and_save_on_s3(
        s3hook=s3hook,
        source_s3_bucket=unprocessed_bucket,
        all_filenames=generations_filenames,
        objects_per_file=objects_per_file,
        object_read_function=json_to_generation,
        processed_encoder_class=GenerationProcessedEncoder,
        result_files_prefix="processed_generations",
        target_s3_prefix=processed_prefix
    )


def _process_moves(unprocessed_prefix: str, processed_prefix: str, objects_per_file: int):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    moves_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    combine_files_into_one_and_save_on_s3(
        s3hook=s3hook,
        source_s3_bucket=unprocessed_bucket,
        all_filenames=moves_filenames,
        objects_per_file=objects_per_file,
        object_read_function=json_to_move,
        processed_encoder_class=MoveProcessedEncoder,
        result_files_prefix="processed_moves",
        target_s3_prefix=processed_prefix
    )


def _process_stats(unprocessed_prefix: str, processed_prefix: str, objects_per_file: int):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    stats_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    combine_files_into_one_and_save_on_s3(
        s3hook=s3hook,
        source_s3_bucket=unprocessed_bucket,
        all_filenames=stats_filenames,
        objects_per_file=objects_per_file,
        object_read_function=json_to_stat,
        processed_encoder_class=StatProcessedEncoder,
        result_files_prefix="processed_stats",
        target_s3_prefix=processed_prefix
    )


def _process_types(unprocessed_prefix: str, processed_prefix: str, objects_per_file: int):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    types_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    combine_files_into_one_and_save_on_s3(
        s3hook=s3hook,
        source_s3_bucket=unprocessed_bucket,
        all_filenames=types_filenames,
        objects_per_file=objects_per_file,
        object_read_function=json_to_type,
        processed_encoder_class=TypeProcessedEncoder,
        result_files_prefix="processed_types",
        target_s3_prefix=processed_prefix
    )


def copy_directory_content_from_s3_to_s3(source_s3_prefix: str, target_s3_prefix: str):
    s3hook = S3Hook()
    print(f"source prefix = {source_s3_prefix}, dest_prefix = {target_s3_prefix}")
    #
    source_bucket, source_key = S3Hook.parse_s3_url(source_s3_prefix)
    print(f"source: bucket = {source_bucket}, key = {source_key}")
    #
    target_bucket, target_key = S3Hook.parse_s3_url(target_s3_prefix)
    print(f"target: bucket = {target_bucket}, key = {target_key}")
    source_keys = get_filenames_from_s3(s3hook, source_bucket, source_key)
    print(f"copying {len(source_keys)} keys:")
    for filename in source_keys:
        target_filename = os.path.join(target_key, os.path.split(filename)[-1])
        print(f"copying {filename} TO {target_bucket}, {target_filename}")
        s3hook.copy_object(
            source_bucket_key=filename,
            source_bucket_name=source_bucket,
            dest_bucket_key=target_filename,
            dest_bucket_name=target_bucket
        )


def _load_pokemons(processed_s3_prefix: str, snowflake_s3_prefix: str):
    copy_directory_content_from_s3_to_s3(processed_s3_prefix, snowflake_s3_prefix)


def _load_generations(processed_s3_prefix: str, snowflake_s3_prefix: str):
    copy_directory_content_from_s3_to_s3(processed_s3_prefix, snowflake_s3_prefix)


def _load_moves(processed_s3_prefix: str, snowflake_s3_prefix: str):
    copy_directory_content_from_s3_to_s3(processed_s3_prefix, snowflake_s3_prefix)


def _load_stats(processed_s3_prefix: str, snowflake_s3_prefix: str):
    copy_directory_content_from_s3_to_s3(processed_s3_prefix, snowflake_s3_prefix)


def _load_types(processed_s3_prefix: str, snowflake_s3_prefix: str):
    copy_directory_content_from_s3_to_s3(processed_s3_prefix, snowflake_s3_prefix)


def _cleanup(list_of_directories_to_clean):
    s3hook = S3Hook()
    for s3_url in list_of_directories_to_clean:
        bucket, key = S3Hook.parse_s3_url(s3_url)
        keys = s3hook.list_keys(bucket_name=bucket, prefix=key)
        print(f"cleaning {s3_url}, number of keys to be deleted {len(keys)}")
        for b_path in keys:
            prefix, filename = os.path.split(b_path)
            if filename:
                s3hook.delete_objects(bucket=bucket, keys=b_path)
            else:
                print(f"not a file: {b_path}")
