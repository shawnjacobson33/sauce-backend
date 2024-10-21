import sys
import time
import uuid

from app.product_data.data_sourcing.utils import get_db, Bookmaker, get_bookmaker, RequestManager, DataStandardizer, \
    IN_SEASON_LEAGUES


async def run(plug):
    with open('logs/standardizing_log.txt', 'w') as f:
        sys.stdout = f
        db = get_db()
        batch_id = str(uuid.uuid4())
        print(f'Batch ID: {batch_id}')
        bookmaker_info = Bookmaker(get_bookmaker(db, plug.__name__))
        plug_obj = plug(bookmaker_info, batch_id, RequestManager(use_requests=(plug.__name__ == 'BetOnline')), DataStandardizer(batch_id, db, has_grouping=(plug.__name__ != 'Drafters')))
        start_time = time.time()
        await plug_obj.start()
        end_time = time.time()

    sys.stdout = sys.__stdout__
    print(f'[{plug.__name__}]: {plug_obj} lines, {round(end_time - start_time, 2)}s')


def is_league_good(league) -> bool:
    return league in IN_SEASON_LEAGUES