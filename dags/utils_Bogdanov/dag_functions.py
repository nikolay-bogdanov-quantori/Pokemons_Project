import os
import json
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils_Bogdanov.fetching_funcs import _fetch_api_url_async
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


def load_string_on_s3(s3hook, data: str, key: str) -> None:
    s3hook.load_string(string_data=data, key=key, replace=True)


def read_json_from_s3(s3hook, bucket, key):
    return json.loads(s3hook.read_key(key=key, bucket_name=bucket))


def get_filenames_from_s3(s3hook, bucket, key):
    files_s3_urls = [key for key in s3hook.list_keys(bucket_name=bucket, prefix=key) if os.path.split(key)[-1]]
    return files_s3_urls


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
                      processed_pokemons_prefix: str):
    s3hook = S3Hook()
    unprocessed_pokemons_bucket, unprocessed_pokemons_key = S3Hook.parse_s3_url(unprocessed_pokemons_prefix)
    unprocessed_generations_bucket, unprocessed_generations_key = S3Hook.parse_s3_url(unprocessed_generations_prefix)
    #
    pokemons_filenames = get_filenames_from_s3(s3hook, unprocessed_pokemons_bucket, unprocessed_pokemons_key)
    generations_filenames = get_filenames_from_s3(s3hook, unprocessed_generations_bucket, unprocessed_generations_key)
    #
    species_to_generations: dict[str, int] = {}
    for generation_filename in generations_filenames:
        print(f"reading {generation_filename}")
        json_string = read_json_from_s3(s3hook, unprocessed_generations_bucket, generation_filename)
        generation: Generation = json_to_generation(json_string)
        for species in generation.species:
            species_to_generations[species] = generation.id
    #
    pokemons_partitioned_by_generations: dict[int, list[Pokemon]] = {}
    for pokemon_filename in pokemons_filenames:
        print(f"reading {pokemon_filename}")
        json_string = read_json_from_s3(s3hook, unprocessed_pokemons_bucket, pokemon_filename)
        pokemon: Pokemon = json_to_pokemon(json_string)
        pokemon.generation = species_to_generations[pokemon.species]
        if pokemon.generation not in pokemons_partitioned_by_generations:
            pokemons_partitioned_by_generations[pokemon.generation] = []
        pokemons_partitioned_by_generations[pokemon.generation].append(pokemon)
    #
    for generation_id, pokemon_list in pokemons_partitioned_by_generations.items():
        filename = os.path.join(processed_pokemons_prefix, f"processed_pokemons_generation_{generation_id}.json")
        json_string = json.dumps(pokemon_list, cls=PokemonProcessedEncoder, indent=4)
        load_string_on_s3(s3hook, data=json_string, key=filename)


def _process_generations(unprocessed_prefix: str, processed_prefix: str):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    generations_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    generations_list = [None]*len(generations_filenames)
    for j, filename in enumerate(generations_filenames):
        json_string = read_json_from_s3(s3hook, unprocessed_bucket, filename)
        generations_list[j] = json_to_generation(json_string)
    #
    processed_filename = os.path.join(processed_prefix, "processed_generations.json")
    json_string = json.dumps(generations_list, cls=GenerationProcessedEncoder, indent=4)
    load_string_on_s3(s3hook, data=json_string, key=processed_filename)


def _process_moves(unprocessed_prefix: str, processed_prefix: str):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    moves_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    moves_list = [None]*len(moves_filenames)
    for j, filename in enumerate(moves_filenames):
        json_string = read_json_from_s3(s3hook, unprocessed_bucket, filename)
        moves_list[j] = json_to_move(json_string)
    #
    processed_filename = os.path.join(processed_prefix, "processed_moves.json")
    json_string = json.dumps(moves_list, cls=MoveProcessedEncoder, indent=4)
    load_string_on_s3(s3hook, data=json_string, key=processed_filename)


def _process_stats(unprocessed_prefix: str, processed_prefix: str):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    stats_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    stats_list = [None]*len(stats_filenames)
    for j, filename in enumerate(stats_filenames):
        json_string = read_json_from_s3(s3hook, unprocessed_bucket, filename)
        stats_list[j] = json_to_stat(json_string)
    #
    processed_filename = os.path.join(processed_prefix, "processed_stats.json")
    json_string = json.dumps(stats_list, cls=StatProcessedEncoder, indent=4)
    load_string_on_s3(s3hook, data=json_string, key=processed_filename)


def _process_types(unprocessed_prefix: str, processed_prefix: str):
    s3hook = S3Hook()
    unprocessed_bucket, unprocessed_key = S3Hook.parse_s3_url(unprocessed_prefix)
    types_filenames = get_filenames_from_s3(s3hook, unprocessed_bucket, unprocessed_key)
    types_list = [None]*len(types_filenames)
    for j, filename in enumerate(types_filenames):
        json_string = read_json_from_s3(s3hook, unprocessed_bucket, filename)
        types_list[j] = json_to_type(json_string)
    #
    processed_filename = os.path.join(processed_prefix, "processed_types.json")
    json_string = json.dumps(types_list, cls=TypeProcessedEncoder, indent=4)
    load_string_on_s3(s3hook, data=json_string, key=processed_filename)


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
