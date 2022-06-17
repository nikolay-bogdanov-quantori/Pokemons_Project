import os
from typing import Any
import requests
import json
import time
import asyncio
import aiohttp
import math
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils_Bogdanov.pokemon import Pokemon, PokemonEncoder


def load_string_on_s3(data: str, key: str) -> None:
    s3hook = S3Hook()
    s3hook.load_string(string_data=data, key=key, replace=True)


def split_list_into_chunks_by_size_of_partition(list_to_split, partition_size: int):
    return [list_to_split[j:j + partition_size] for j in range(0, len(list_to_split), partition_size)]


def split_list_into_chunks_by_amount_of_partitions(list_to_split, amount_of_parts: int):
    partition_size = math.ceil(len(list_to_split)/amount_of_parts)
    return split_list_into_chunks_by_size_of_partition(list_to_split, partition_size)


def _start() -> None:
    print("Dag started")


def _end_success() -> None:
    print('Dag has finished successfully!')


def _end_fail() -> None:
    print('Dag execution failed!')


async def get_pokemons_multithread_async(pokemons_urls, threads_count, requests_per_second, unprocessed_files_directory):
    async def get_pokemons_onethread_async(urls):
        async def get_one_pokemon_async(url):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.ok:
                        json_content = await response.json()
                        data = json.dumps(json_content)
                        filename = f"pokemon_unprocessed_{json_content['name']}.json"
                        s3_url = os.path.join(unprocessed_files_directory, filename)
                        load_string_on_s3(data, s3_url)
                        print(f"fetched {s3_url}")
                    else:
                        print(f"{url} failed to load json!")
        urls_partitions: list[list[str]] = split_list_into_chunks_by_size_of_partition(
            urls,
            partition_size=requests_per_second
        )
        for partition in urls_partitions:
            await asyncio.gather(*(get_one_pokemon_async(url) for url in partition))
            # TODO добавить лимит на число запросов в секунду
    # разбиение полученого списка урлов по потокам, в зависимости от их количества
    partitioned_urls = split_list_into_chunks_by_amount_of_partitions(pokemons_urls, threads_count)
    await asyncio.gather(*(get_pokemons_onethread_async(p_urls) for p_urls in partitioned_urls))


def _fetch_pokemons(
        api_pokemons_catalog_url: str,
        unprocessed_s3_prefix: str,
        threads_count: int,
        requests_per_second: int):
    # получение списка urls по покемонам
    print(f"url to fetch {api_pokemons_catalog_url}")
    pokemons_json_response = requests.get(api_pokemons_catalog_url).json()
    print(f"fetching {pokemons_json_response['count']} pokemons from API and saving to {unprocessed_s3_prefix}")
    pokemons_urls = [""] * pokemons_json_response["count"]
    pokemons_urls_index = 0
    while True:
        # TODO добавить лимит на число запросов в секунду
        for pokemon_dict in pokemons_json_response['results']:
            pokemons_urls[pokemons_urls_index] = pokemon_dict['url']
            pokemons_urls_index += 1
        next_page_url = pokemons_json_response['next']
        if next_page_url is None:
            break
        pokemons_json_response = requests.get(next_page_url).json()
    # ассинхронная обработка полученного списка урлов:
    asyncio.run(get_pokemons_multithread_async(
        pokemons_urls, threads_count, requests_per_second, unprocessed_s3_prefix)
    )


async def process_pokemons_multithread_async(threads_count, s3_bucketname, s3_filenames):
    async def process_pokemon_jsons_onethread_async(s3_filenames_part):
        s3hook = S3Hook()
        converted_pokemons: list[Pokemon] = [Pokemon() for _ in range(len(s3_filenames_part))]
        for j, pokemon_json_filename in enumerate(s3_filenames_part):
            print(f"processing {pokemon_json_filename}")
            json_content = json.loads(s3hook.read_key(bucket_name=s3_bucketname, key=pokemon_json_filename))
            converted_pokemons[j].get_from_json(json_content)
        return converted_pokemons
    #
    partitioned_filenames = split_list_into_chunks_by_amount_of_partitions(s3_filenames, threads_count)
    converted_pokemons_parts = await asyncio.gather(
        *(process_pokemon_jsons_onethread_async(filenames_list_part) for filenames_list_part in partitioned_filenames)
    )
    joined_pokemons_list = [element for partition in converted_pokemons_parts for element in partition]
    return joined_pokemons_list


def _process_pokemons(unprocessed_s3_prefix, processed_s3_prefix, threads_count, pokemons_per_file):
    s3hook = S3Hook()
    bucket, key = S3Hook.parse_s3_url(unprocessed_s3_prefix)
    keys = [key for key in s3hook.list_keys(bucket_name=bucket, prefix=key) if os.path.split(key)[-1]]
    print(f"number of files: {len(keys)}")
    print(keys)
    pokemons_list = asyncio.run(
        process_pokemons_multithread_async(threads_count=threads_count, s3_bucketname=bucket, s3_filenames=keys)
    )
    print(f"number of processed pokemons: {len(pokemons_list)}")
    partitioned_pokemons_list = split_list_into_chunks_by_size_of_partition(pokemons_list, pokemons_per_file)
    for j, pokemons_list_partition in enumerate(partitioned_pokemons_list):
        filename = f"pokemon_processed_{j+1}.json"
        s3_url = os.path.join(processed_s3_prefix, filename)
        json_string = json.dumps(pokemons_list_partition, cls=PokemonEncoder, indent=4)
        load_string_on_s3(json_string, s3_url)


def _load_pokemons(processed_s3_prefix, snowflake_s3_prefix):
    s3hook = S3Hook()
    print(f"source prefix = {processed_s3_prefix}, dest_prefix = {snowflake_s3_prefix}")
    #
    source_bucket, source_key = S3Hook.parse_s3_url(processed_s3_prefix)
    print(f"bucket = {source_bucket}, key = {source_key}")
    #
    dest_bucket, dest_key = S3Hook.parse_s3_url(snowflake_s3_prefix)
    print(f"bucket = {dest_bucket}, key = {dest_key}")
    #
    source_keys = s3hook.list_keys(bucket_name=source_bucket, prefix=source_key)
    print(f"source keys {source_keys}")
    print(f"copying {len(source_keys)} keys:")
    for b_path in source_keys:
        prefix, filename = os.path.split(b_path)
        if filename:
            print(f"copying {b_path} TO {dest_bucket}, {os.path.join(dest_key, filename)}")
            s3hook.copy_object(
                source_bucket_key=b_path,
                source_bucket_name=source_bucket,
                dest_bucket_key=os.path.join(dest_key, filename),
                dest_bucket_name=dest_bucket
            )
        else:
            print(f"not a file: {b_path}")


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
