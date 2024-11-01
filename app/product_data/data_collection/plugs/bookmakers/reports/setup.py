import asyncio
import json
import os
import time
import uuid

from app.db.database import Database
from app.db.queries import get_bookmaker
from app.product_data.data_collection.shared_data import BettingLines
from app.product_data.data_collection.utils import Bookmaker, RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug


def configure(plug) -> BookmakerPlug:
    db = Database.get()
    batch_id = str(uuid.uuid4())
    bookmaker_info = Bookmaker(get_bookmaker(db, plug.__name__))
    return plug(bookmaker_info, batch_id, RequestManager(use_requests=(plug.__name__ == 'BetOnline')))


def save_betting_lines_to_file(name: str):
    file_path = f'reporting/logs/{name}/betting_lines.json'
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        json.dump(BettingLines.get(), f, indent=4)


def cleanup(plug: BookmakerPlug):
    def decorator(func):
        def wrapper(*args, **kwargs):
            t1 = time.time()
            result = func(*args, **kwargs)
            t2 = time.time()
            plug.report(f"METRIC ->>>>> TIME: {t2 - t1}s")
            plug.report(f"METRIC ->>>>> # of BETTING LINES: {plug.betting_lines_collected}")
            save_betting_lines_to_file(plug.bookmaker_info.name)
            return result

        return wrapper

    return decorator


async def start(plug_class):
    bookmaker_plug = configure(plug_class)

    @cleanup(bookmaker_plug)
    async def collect():
        await bookmaker_plug.collect()
        for market, count in bookmaker_plug.markets_tracker.items():
            bookmaker_plug.report(f"MARKET ->>>>> FOUND: {market} ({count})")

        for subject, count in bookmaker_plug.subjects_tracker:
            bookmaker_plug.report(f"SUBJECT ->>>>> FOUND: {subject} ({count})")

    await collect()


def run(plug_class):
    asyncio.run(start(plug_class))