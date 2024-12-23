import time
from datetime import datetime
from typing import Optional

import redis


def get_entity_type(name: str) -> str:
    if name == 'teams': return 't'
    elif name == 'subjects': return 's'

    raise ValueError(f"Invalid entity type: {name}")



def convert_to_timestamp(dt: datetime) -> int:
    return int(time.mktime(dt.timetuple()))
