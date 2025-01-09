import hashlib


class Hashing:

    @staticmethod
    def get_subject_key(subject: dict, attribute_field: str) -> str:
        if attribute_field == 'team':
            if not (subject_attribute := subject['team']['abbr_name']):
                raise ValueError(f"Subject missing attribute field '{attribute_field}'")
        else:
            if not (subject_attribute := subject.get(attribute_field)):
                raise ValueError(f"Subject missing attribute field '{attribute_field}'")

        return f"{subject['league']}:{subject_attribute}:{subject['name']}"

    @staticmethod
    def get_betting_line_key(line: dict) -> str:
        return f"{line['bookmaker']}:{line['league']}:{line['market']}:{line['subject']}:{line['label']}"

    @staticmethod
    def generate_hash(key: str):
        return hashlib.sha256(key.encode()).hexdigest()