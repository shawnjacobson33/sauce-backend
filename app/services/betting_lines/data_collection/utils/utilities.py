import hashlib

from app.services.betting_lines.data_collection.utils.standardization import Standardization
from app.services.betting_lines.data_collection.utils.requesting.requesting import Requesting


requester = Requesting()
standardizer = Standardization()


def get_betting_line_key(line: dict) -> str:
    return f"{line['bookmaker']}:{line['league']}:{line['market']}:{line['subject']}:{line['label']}"


def generate_unique_id(key: str):
    return hashlib.sha256(key.encode()).hexdigest()