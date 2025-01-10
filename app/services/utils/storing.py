from datetime import datetime


class Storing:

    @staticmethod
    def get_subject_key(league: str, subject_name: str, subject_attribute: str = None) -> str:
        if subject_attribute:
            return f"{league}:{subject_attribute}:{subject_name}"

        return f"{league}:{subject_name}"

    @staticmethod
    def get_betting_line_key(line: dict) -> str:
        return f"{line['bookmaker']}:{line['league']}:{line['market']}:{line['subject']}:{line['label']}"
