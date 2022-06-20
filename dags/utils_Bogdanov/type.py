import json
from utils_Bogdanov.i_api_fetchable import IApiParserJson


class Type:
    def __init__(self):
        self.id = None
        self.name = None

    def __str__(self):
        return f"{self.__dict__}\n"


class TypeApiParserJson(IApiParserJson):
    @staticmethod
    def parse(json_repr: dict) -> Type:
        result = Type()
        result.id = json_repr['id']
        result.name = json_repr['name']
        return result


class TypeUnprocessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Type):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class TypeProcessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Type):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


def json_to_type(json_repr) -> Type:
    result = Type()
    result.id = json_repr['id']
    result.name = json_repr['name']
    return result
