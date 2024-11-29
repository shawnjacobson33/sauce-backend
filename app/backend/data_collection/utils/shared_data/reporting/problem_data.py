import threading
from collections import defaultdict


class ProblemData:
    _problem_subjects: defaultdict[tuple, dict] = defaultdict(dict)
    _problem_teams: defaultdict[tuple, dict] = defaultdict(dict)
    _problem_markets: defaultdict[tuple, dict] = defaultdict(dict)
    _lock1: threading.Lock = threading.Lock()
    _lock2: threading.Lock = threading.Lock()
    _lock3: threading.Lock = threading.Lock()

    @classmethod
    def get_problem_subjects(cls, source_name: str = None) -> dict:
        return {key: val for key, val in cls._problem_subjects.items() if key[0] == source_name} \
            if source_name else cls._problem_subjects

    @classmethod
    def get_problem_teams(cls, source_name: str = None) -> dict:
        return {key: val for key, val in cls._problem_teams.items() if key[0] == source_name} \
            if source_name else cls._problem_teams

    @classmethod
    def get_problem_markets(cls, source_name: str = None) -> dict:
        return {key: val for key, val in cls._problem_markets.items() if key[0] == source_name} \
            if source_name else cls._problem_markets

    @classmethod
    def update_problem_subjects(cls, subject: dict, source_name: str) -> None:
        with cls._lock1:
            if spec_subject_attr := [val for attr, val in subject.items() if attr not in {'name', 'league'}]:
                cls._problem_subjects[(source_name, subject['league'], spec_subject_attr[0], subject['name'])] = subject
            else:
                cls._problem_subjects[(source_name, subject['league'], subject['name'])] = subject

    @classmethod
    def update_problem_teams(cls, team: dict, source_name: str) -> None:
        with cls._lock2:
            cls._problem_teams[(source_name, team['league'], team['abbr_name'])] = team

    @classmethod
    def update_problem_markets(cls, market: dict, source_name: str) -> None:
        with cls._lock3:
            cls._problem_markets[(source_name, market['sport'], market['name'])] = market