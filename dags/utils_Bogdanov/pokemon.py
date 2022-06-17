import json


class Pokemon:
    def __init__(self):
        self.name = None
        self.types = None
        self.moves = None
        self.stats = None
        self.generations = None

    def get_from_json(self, json_repr: dict):
        # name просто получаем по ключу:
        self.name = json_repr['name']
        # types: список вида
        # [{'slot': 1, 'type': {'name': 'grass', 'url': '...'}}, {'slot': 2, 'type': {'name': 'poison', 'url': '...'}}]
        # из всего этого нам необходимы только types['type']['name']
        self.types = [p_type['type']['name'] for p_type in json_repr['types']]

    def __str__(self):
        return f"name: {self.name}, types: {self.types}\n"


class PokemonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Pokemon):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)

