import sys
import time
import uuid

from app.db.database import Database
from app.db.queries import get_bookmaker
from app.product_data.data_collection.utils import Bookmaker, RequestManager


async def run(plug):
    with open('logs/standardizing_log.txt', 'w') as f:
        sys.stdout = f
        db = Database.get()
        batch_id = str(uuid.uuid4())
        print(f'Batch ID: {batch_id}')
        bookmaker_info = Bookmaker(get_bookmaker(db, plug.__name__))
        plug_obj = plug(bookmaker_info, batch_id, RequestManager(use_requests=(plug.__name__ == 'BetOnline')))
        start_time = time.time()
        await plug_obj.start()
        end_time = time.time()

    sys.stdout = sys.__stdout__
    print(f'[{plug.__name__}]: {plug_obj} lines, {round(end_time - start_time, 2)}s')
