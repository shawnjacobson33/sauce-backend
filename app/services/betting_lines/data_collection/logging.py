import time
import functools

from app.db import db


def collector_logger(self, message: str):

    def decorator(collection_func):

        @functools.wraps(collection_func)  # Preserves original function metadata
        async def wrapper(*args, **kwargs):
            print(f'[BettingLines] [Collection] [{self.name}]: {message}...')
            start_time = time.time()
            await collection_func(*args, **kwargs)
            end_time = time.time()
            print(f'[BettingLines] [Collection] [{self.name}]: Finished {message}...', round(end_time - start_time, 2))

            await db.betting_lines_pipeline_stats.add_batch_stat('data_collection')

        return wrapper

    return decorator


def data_collection_main_logger(message: str):

    def decorator(collection_func):

        @functools.wraps(collection_func)  # Preserves original function metadata
        async def wrapper(*args, **kwargs):
            print(f'[BettingLines] [Collection]: {message}...')
            start_time = time.time()
            collection_stats, collected_betting_lines = await collection_func(*args, **kwargs)
            end_time = time.time()
            print(f'[BettingLines] [Collection]: Finished {message}...', round(end_time - start_time, 2))

            await db.betting_lines_pipeline_stats.add_batch_stat('data_collection', 'start_time', start_time)
            await db.betting_lines_pipeline_stats.add_batch_stat('data_collection', 'end_time', end_time)

            return collected_betting_lines

        return wrapper

    return decorator
