import threading
from collections import defaultdict


class RelevantData:
    _relevant_subjects: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _relevant_teams: defaultdict[str, dict] = defaultdict(lambda: defaultdict(dict))
    _lock1: threading.Lock = threading.Lock()
    _lock2: threading.Lock = threading.Lock()

    @classmethod
    def get_relevant_subjects(cls, league: str = None, source_name: str = None) -> dict:
        return cls._relevant_subjects.get(league) if league and source_name else cls._relevant_subjects

    @classmethod
    def get_relevant_teams(cls, league: str = None, source_name: str = None) -> dict:
        return cls._relevant_teams.get(league) if league and source_name else cls._relevant_teams

    @classmethod
    def update_relevant_subjects(cls, subject: dict, source_name: str, league: str) -> None:
        with cls._lock1:
            partitioned_subjects = cls._relevant_subjects[source_name][league]
            partitioned_subjects[subject['id']] = {key: value for key, value in subject.items() if
                                                             key != 'id'}

    @classmethod
    def update_relevant_teams(cls, team: dict, source_name: str, league: str) -> None:
        with cls._lock1:
            cls._relevant_teams[source_name][league][team['id']] = {key: value for key, value in team.items() if
                                                             key != 'id'}