import json
from utils_Bogdanov.i_api_fetchable import IApiParserJson


class Move:
    def __init__(self):
        self.id = None
        self.name = None

    def __str__(self):
        return f"{self.__dict__}\n"


class MoveApiParserJson(IApiParserJson):
    @staticmethod
    def parse(json_repr: dict) -> Move:
        result = Move()
        result.id = json_repr['id']
        result.name = json_repr['name']
        return result


class MoveUnprocessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Move):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class MoveProcessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Move):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


def json_to_move(json_repr) -> Move:
    result = Move()
    result.id = json_repr['id']
    result.name = json_repr['name']
    return result
