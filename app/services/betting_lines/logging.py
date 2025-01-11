import time
import functools

from app.db import db


def data_collection_component_logger(collector: str, message: str):

    def decorator(collection_func):

        @functools.wraps(collection_func)  # Preserves original function metadata
        async def wrapper(*args, **kwargs):
            print(f'[BettingLines] [Collection] [{collector}]: {message}...')
            start_time = time.time()
            result = await collection_func(*args, **kwargs)
            end_time = time.time()
            print(f'[BettingLines] [Collection] [{collector}]: Finished {message}...', round(end_time - start_time, 2))

            return result

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
