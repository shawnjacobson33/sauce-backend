import threading
from collections import defaultdict


def restructure_sets(data: dict) -> dict:
    restructured_data = dict()
    for key, values in data.items():
        subjects = list()
        for value in values:
            value_dict = dict()
            for attribute in value:
                value_dict[attribute[0]] = attribute[1]

            subjects.append(value_dict)

        restructured_data[key] = subjects

    return restructured_data


class Leagues:
    _stored_data: dict  # Dictionary is much faster than any other data structure.
    _valid_data: dict = defaultdict(set)
    _pending_data: dict = defaultdict(set)  # Hold data that needs to be evaluated manually before db insertion
    _lock1 = threading.Lock()
    _lock2 = threading.Lock()
    _lock3 = threading.Lock()

    @classmethod
    def get_stored_data(cls):
        return cls._stored_data

    @classmethod
    def get_pending_data(cls) -> dict:
        return restructure_sets(cls._pending_data)

    @classmethod
    def get_valid_data(cls) -> dict:
        return restructure_sets(cls._valid_data)

    @classmethod
    def update_stored_data(cls, key, value):
        with cls._lock1:
            cls._stored_data[key] = value

    @classmethod
    def update_pending_data(cls, key: str, data: tuple):
        with cls._lock2:
            cls._pending_data[key].add(data)

    @classmethod
    def update_valid_leagues(cls, bookmaker: str, league: str):
        with cls._lock1:
            cls._valid_data[bookmaker].add((('name', league),))
