from abc import ABC, abstractmethod
from typing import Any


class IApiParser(ABC):
    @staticmethod
    @abstractmethod
    def parse(content: Any):
        pass


class IApiParserJson(IApiParser):
    @staticmethod
    @abstractmethod
    def parse(json_repr: dict):
        pass




