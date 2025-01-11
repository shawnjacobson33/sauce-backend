import functools
import time

from app.db import db


def data_processing_logger(message: str):

    def decorator(processing_func):

        @functools.wraps(processing_func)
        async def wrapped(*args, **kwargs):
            print(f'[BettingLines] [Processing]: {message}...')
            start_time = time.time()
            result = processing_func(*args, **kwargs)
            end_time = time.time()
            print(f'[BettingLines] [Processing]: Finished {message}...')

            await db.betting_lines_pipeline_stats.add_batch_stat('data_processing_start_time', start_time)
            await db.betting_lines_pipeline_stats.add_batch_stat('data_processing_end_time', end_time)

            return result

        return wrapped

    return decorator