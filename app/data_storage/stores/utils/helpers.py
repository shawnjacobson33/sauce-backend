import time
from datetime import datetime
from typing import Optional

import redis


def get_entity_type(name: str) -> str:
    return name.split(':')[0][:-1]


def convert_to_timestamp(dt: datetime) -> int:
    return int(time.mktime(dt.timetuple()))
