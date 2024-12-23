import time
from datetime import datetime
from typing import Optional

import redis


def get_entity_type(name: str) -> str:
    return name.split(':')[0]
