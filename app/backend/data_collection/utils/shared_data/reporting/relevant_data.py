import threading
from collections import defaultdict, deque


class RelevantData:
    _relevant_leagues: defaultdict[str, deque] = defaultdict(deque)
    _relevant_subjects: defaultdict[tuple, dict] = defaultdict(dict)
    _relevant_teams: defaultdict[tuple, dict] = defaultdict(dict)
    _relevant_markets: defaultdict[tuple, dict] = defaultdict(dict)
    _lock1: threading.Lock = threading.Lock()
    _lock2: threading.Lock = threading.Lock()
    _lock3: threading.Lock = threading.Lock()
    _lock4: threading.Lock = threading.Lock()

    @classmethod
    def get_relevant_subjects(cls, source_name: str = None, league: str = None) -> dict:
        return {key: val for key, val in cls._relevant_subjects.items() if key[0] == source_name}

    @classmethod
    def get_relevant_teams(cls, source_name: str = None, league: str = None) -> dict:
        return {key: val for key, val in cls._relevant_teams.items() if key[0] == source_name}

    @classmethod
    def get_relevant_markets(cls, source_name: str = None, league: str = None) -> dict:
        return {key: val for key, val in cls._relevant_markets.items() if key[0] == source_name}

    @classmethod
    def get_relevant_leagues(cls, source_name: str = None) -> dict:
        return cls._relevant_leagues[source_name] if source_name else cls._relevant_leagues

    @classmethod
    def update_relevant_subjects(cls, subject: dict, source_name: str) -> None:
        with cls._lock1:
            spec_subject_attr = [val for attr, val in subject.items() if attr not in {'name', 'league'}][0]
            cls._relevant_subjects[(source_name, subject['league'], spec_subject_attr, subject['name'])] = subject

    @classmethod
    def update_relevant_teams(cls, team: dict, source_name: str) -> None:
        with cls._lock2:
            cls._relevant_teams[(source_name, team['league'], team['abbr_name'])] = team

    @classmethod
    def update_relevant_markets(cls, market: dict, source_name: str) -> None:
        with cls._lock3:
            cls._relevant_markets[(source_name, market['sport'], market['name'])] = market

    @classmethod
    def update_relevant_leagues(cls, league: str, source_name: str) -> None:
        with cls._lock4:
            cls._relevant_leagues[source_name].append(league)