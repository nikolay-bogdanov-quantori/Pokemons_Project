import json
from utils_Bogdanov.i_api_fetchable import IApiParserJson


class Generation:
    def __init__(self):
        self.id = None
        self.name = None
        self.species = None

    def __str__(self):
        return f"{self.__dict__}\n"


class GenerationApiParserJson(IApiParserJson):
    @staticmethod
    def parse(json_repr: dict) -> Generation:
        result = Generation()
        result.id = json_repr['id']
        result.name = json_repr['name']
        result.species = [g_species['name'] for g_species in json_repr['pokemon_species']]
        return result


class GenerationUnprocessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Generation):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class GenerationProcessedEncoder(json.JSONEncoder):
    def default(self, obj):
        # не включаем в итоговый словарь избыточный атрибут generation.species
        if isinstance(obj, Generation):
            result = {
                'id': obj.id,
                'name': obj.name,
            }
            return result
        return json.JSONEncoder.default(self, obj)


def json_to_generation(json_repr) -> Generation:
    result = Generation()
    result.id = json_repr['id']
    result.name = json_repr['name']
    result.species = [g_species for g_species in json_repr['species']]
    return result
