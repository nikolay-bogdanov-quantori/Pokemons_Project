import json
from utils_Bogdanov.i_api_fetchable import IApiParserJson


class Stat:
    def __init__(self):
        self.id = None
        self.name = None

    def __str__(self):
        return f"{self.__dict__}\n"


class StatApiParserJson(IApiParserJson):
    @staticmethod
    def parse(json_repr: dict):
        result = Stat()
        result.id = json_repr['id']
        result.name = json_repr['name']
        return result


class StatUnprocessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Stat):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class StatProcessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Stat):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


def json_to_stat(json_repr):
    result = Stat()
    result.id = json_repr['id']
    result.name = json_repr['name']
    return result
