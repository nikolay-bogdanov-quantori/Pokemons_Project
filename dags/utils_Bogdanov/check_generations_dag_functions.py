import requests


def _check_generations_count(generations_api_catalog_url: str) -> None:
    json_response = requests.get(generations_api_catalog_url).json()
    print(f"Today {generations_api_catalog_url} consists {json_response['count']} generations!")


def _start() -> None:
    print("Dag started")


def _end_success() -> None:
    print('Dag has finished successfully!')


def _end_fail() -> None:
    print('Dag execution failed!')
