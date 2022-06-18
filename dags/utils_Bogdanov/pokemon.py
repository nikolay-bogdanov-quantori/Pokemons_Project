import json
from utils_Bogdanov.i_api_fetchable import IApiParserJson


class Pokemon:
    def __init__(self):
        self.name = None
        self.types = None
        self.moves = None
        self.stats = None
        self.species = None
        self.generation = None
        self.past_types = None

    def __str__(self):
        return f"{self.__dict__}"


class PokemonApiParserJson(IApiParserJson):
    @staticmethod
    def parse(json_repr: dict):
        result = Pokemon()
        result.name = json_repr['name']
        result.types = [p_type['type']['name'] for p_type in json_repr['types']]
        result.moves = [p_move['move']['name'] for p_move in json_repr['moves']]
        result.stats = [{p_stat['stat']['name']: p_stat['base_stat']} for p_stat in json_repr['stats']]
        result.species = json_repr['species']['name']
        past_types_list: list[dict[str, list[str]]] = [
            {past_types['generation']['name']: [p_type['type']['name'] for p_type in past_types['types']]}
            for past_types in json_repr['past_types']]
        result.past_types = past_types_list
        return result


class PokemonUnprocessedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Pokemon):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class PokemonProcessedEncoder(json.JSONEncoder):
    def default(self, obj: Pokemon):
        if isinstance(obj, Pokemon):
            result = {
                'name': obj.name,
                'types': obj.types,
                'moves': obj.moves,
                'stats': obj.stats,
                'generation': obj.generation,
                'past_types': obj.past_types
            }
            return result
        return json.JSONEncoder.default(self, obj)


def json_to_pokemon(json_repr):
    result = Pokemon()
    result.name = json_repr['name']
    result.types = json_repr['types']
    result.moves = json_repr['moves']
    result.stats = json_repr['stats']
    result.species = json_repr['species']
    result.generation = json_repr['generation']
    result.past_types = json_repr['past_types']
    return result
