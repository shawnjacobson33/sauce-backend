from collections import defaultdict


class Metrics:
    def __init__(self):
        self.__leagues_report = set()
        self.__markets_report = defaultdict(int)
        self.__subjects_report = defaultdict(int)

    def get_leagues_report(self):
        return self.__leagues_report

    def get_markets_report(self):
        return self.__markets_report

    def get_subjects_report(self):
        return self.__subjects_report

    def add_league(self, league: str) -> None:
        self.__leagues_report.add(league)

    def add_market(self, market: tuple[str, str]) -> None:
        self.__markets_report[market] += 1

    def add_subject(self, subject: tuple[str, str]) -> None:
        self.__subjects_report[subject] += 1
