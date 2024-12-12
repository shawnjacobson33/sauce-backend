import time
from datetime import datetime
from typing import Optional

import redis


def convert_to_timestamp(dt: datetime) -> int:
    return int(time.mktime(dt.timetuple()))


def update_live_hash(r: redis.Redis, name: str, live_ids: set[str]) -> None:
    """Add to a set to index data that is live, and update the 'is_live' field to True"""
    for idx in live_ids:
        r.sadd(name, idx)


def get_live_ids(r: redis.Redis, name: str):
    curr_ts = convert_to_timestamp(datetime.now())
    live_ids = r.zrange(
        name=name,
        start=int(float('-inf')),
        end=curr_ts,
        byscore=True
    )
    r.zrem(name, *live_ids)
    return live_ids


def watch_game_time(r: redis.Redis, name: str, key: str, game_time: int) -> None:
    """Keep a sorted set of games by closest to starting"""
    r.zadd(name, mapping={ key: game_time })
