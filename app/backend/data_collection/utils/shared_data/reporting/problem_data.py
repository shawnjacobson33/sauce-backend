import threading
from collections import defaultdict


class ProblemData:
    _problem_subjects: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _problem_teams: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _problem_markets: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _lock1: threading.Lock = threading.Lock()
    _lock2: threading.Lock = threading.Lock()
    _lock3: threading.Lock = threading.Lock()

    @classmethod
    def get_problem_subjects(cls, source_name: str = None, league: str = None) -> dict:
        return cls._problem_subjects[source_name]

    @classmethod
    def get_problem_teams(cls, source_name: str = None, league: str = None) -> dict:
        return cls._problem_teams[source_name]

    @classmethod
    def get_problem_markets(cls, source_name: str = None, league: str = None) -> dict:
        return cls._problem_markets[source_name]

    @classmethod
    def update_problem_subjects(cls, subject: dict, source_name: str) -> None:
        with cls._lock1:
            source_spec_subjects = cls._problem_subjects[source_name]
            spec_subject_attr = [val for attr, val in subject.__dict__.items() if attr not in {'name', 'league'}][0]
            source_spec_subjects[(subject['league'], subject[spec_subject_attr], subject['name'])] = subject

    @classmethod
    def update_problem_teams(cls, team: dict, source_name: str) -> None:
        with cls._lock2:
            source_spec_teams = cls._problem_teams[source_name]
            source_spec_teams[(team['league'], team['abbr_name'])] = team

    @classmethod
    def update_problem_markets(cls, market: dict, source_name: str) -> None:
        with cls._lock3:
            source_spec_markets = cls._problem_markets[source_name]
            source_spec_markets[(market['sport'], market['name'])] = market