import time
from datetime import datetime
from typing import Optional

import redis


def convert_to_timestamp(dt: datetime) -> int:
    return int(time.mktime(dt.timetuple()))


def reset_id_generator(r: redis.Redis, name: str) -> None:
    """Resets the unique id field back to 0"""
    r.set(f'{name}:new:id', 0)


def get_short_id(r: redis.Redis, name: str) -> str:
    short_id = r.get(f'{name}:auto:id')
    r.incrby(f'{name}:auto:id', 1)
    return f'{name[:-1]}:{short_id}'


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
