import threading
from collections import defaultdict


class ProblemData:
    _problem_subjects: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _problem_teams: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _lock1: threading.Lock = threading.Lock()
    _lock2: threading.Lock = threading.Lock()

    @classmethod
    def get_problem_subjects(cls, league: str = None, source_name: str = None) -> dict:
        return cls._problem_subjects[source_name].get(league) if league and source_name else cls._problem_subjects

    @classmethod
    def get_problem_teams(cls, league: str = None, source_name: str = None) -> dict:
        return cls._problem_teams[source_name].get(league) if league and source_name else cls._problem_teams

    @classmethod
    def update_problem_subjects(cls, subject: dict, source_name: str, league: str):
        with cls._lock1:
            partitioned_subjects = cls._problem_subjects[source_name][league]
            partitioned_subjects[subject['name']] = {key: value for key, value in subject.items() if key != 'id'}

    @classmethod
    def update_problem_teams(cls, team: dict, source_name: str, league: str):
        with cls._lock2:
            cls._problem_teams[source_name][league][team['abbr_name']] = {key: value for key, value in team.items() if
                                                                         key != 'id'}
