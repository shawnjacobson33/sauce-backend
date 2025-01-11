import functools
import time

from app.db import db


def processor_logger(name: str, message: str):

    def decorator(processing_func):

        @functools.wraps(processing_func)
        async def wrapped(*args, **kwargs):
            print(f'[BettingLines] [Processing]: {message}...')
            start_time = time.time()
            result = processing_func(*args, **kwargs)
            end_time = time.time()
            print(f'[BettingLines] [Processing]: Finished {message}...')


            db.pipeline_stats.add_processor_stats(name, start_time)

            return result

        return wrapped

    return decorator