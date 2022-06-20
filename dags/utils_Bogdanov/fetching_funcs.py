import os
import requests
import json
import asyncio
import aiohttp
import math
from utils_Bogdanov.i_api_fetchable import IApiParser
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def load_string_on_s3(data: str, key: str) -> None:
    s3hook = S3Hook()
    s3hook.load_string(string_data=data, key=key, replace=True)


def split_list_into_chunks_by_size_of_partition(list_to_split, partition_size: int):
    return [list_to_split[j:j + partition_size] for j in range(0, len(list_to_split), partition_size)]


def split_list_into_chunks_by_amount_of_partitions(list_to_split, amount_of_parts: int):
    partition_size = math.ceil(len(list_to_split)/amount_of_parts)
    return split_list_into_chunks_by_size_of_partition(list_to_split, partition_size)


async def fetch_multithread_async(
        urls,
        threads_count,
        requests_per_second,
        unprocessed_files_directory,
        parser: IApiParser,
        class_encoder: json.JSONEncoder):
    async def fetch_onethread_async(thread_urls):
        async def fetch_one_object_async(url):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.ok:
                        json_content = await response.json()
                        cur_object = parser.parse(json_content)
                        filename = f"unprocessed_{json_content['name']}.json"
                        s3_url = os.path.join(unprocessed_files_directory, filename)
                        json_string = json.dumps(cur_object, cls=class_encoder, indent=4)
                        load_string_on_s3(json_string, s3_url)
                        print(f"fetched {s3_url}")
                    else:
                        print(f"{url} failed to load json!")
        urls_partitions: list[list[str]] = split_list_into_chunks_by_size_of_partition(
            thread_urls,
            partition_size=requests_per_second
        )
        for partition in urls_partitions:
            # ждем ассинхронного завершения корутин, обрабатывающих один объект
            # (их количество в очередной партиции равно requests_per_second) и дополнительной корутины, которая
            # просто ждет одну секунду, таким образом, время выполнения операции ниже будет >= 1 секунды
            await asyncio.gather(*(fetch_one_object_async(url) for url in partition), sleep_one_second_async())
    # разбиение полученого списка урлов по потокам, в зависимости от их количества
    partitioned_urls = split_list_into_chunks_by_amount_of_partitions(urls, threads_count)
    await asyncio.gather(*(fetch_onethread_async(p_urls) for p_urls in partitioned_urls))


def _fetch_api_url_async(
        api_catalog_url: str,
        unprocessed_s3_prefix: str,
        threads_count: int,
        requests_per_second: int,
        parser: IApiParser,
        class_encoder: json.JSONEncoder):
    # получение списка urls из каталога API
    # так как список(массив) получается из связного списка,
    # грузим одним потоком, в отличии от загрузки содержимого самих урлов
    print(f"url to fetch {api_catalog_url}")
    json_response = requests.get(api_catalog_url).json()
    print(f"fetching {json_response['count']} json files from API and saving to {unprocessed_s3_prefix}")
    urls = [""] * json_response["count"]
    urls_index = 0
    page_counter = 1
    while True:
        if page_counter % requests_per_second == 0:  # когда загрузили requests_per_second страниц с урлами,
            asyncio.run(sleep_one_second_async())  # ждем одну секунду
        for pokemon_dict in json_response['results']:
            urls[urls_index] = pokemon_dict['url']
            urls_index += 1
        next_page_url = json_response['next']
        if next_page_url is None:
            break
        json_response = requests.get(next_page_url).json()
        page_counter += 1
    # ассинхронная обработка полученного списка урлов:
    asyncio.run(fetch_multithread_async(
        urls, threads_count, requests_per_second, unprocessed_s3_prefix, parser, class_encoder)
    )


async def sleep_one_second_async() -> None:
    await asyncio.sleep(1)
