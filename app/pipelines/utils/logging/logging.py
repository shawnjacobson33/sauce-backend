import functools
import time

from app.db import db


class Logger:
    
    @staticmethod    
    def collector_logger(message: str):
        def decorator(collection_func):
            @functools.wraps(collection_func)  # Preserves original function metadata
            async def wrapper(self, *args, **kwargs):
                print(f'[{self.domain}] [Collection] [{self.name}]: {message}...')
                start_time = time.time()
                await collection_func(self, *args, **kwargs)
                end_time = time.time()
                print(f'[{self.domain}] [Collection] [{self.name}]: Finished {message}...'
                      f'⏳ {round(end_time - start_time, 2)} seconds ⏳')
                print(
                    f'[{self.domain}] [Collection] [{self.name}]: Collected {len(self.items_container)} {self.domain}...')

                stats = self.get_stats()
                db.pipeline_stats.add_collector_stats(self.name, stats)

                self.num_collected = 0

            return wrapper

        return decorator

    @staticmethod
    def data_collection_main_logger(pipeline: str, message: str):
        def decorator(collection_func):
            @functools.wraps(collection_func)  # Preserves original function metadata
            async def wrapper(*args, **kwargs):
                print(f'[{pipeline}] [Collection]: {message}...')
                start_time = time.time()
                collection_stats, collected_betting_lines = await collection_func(*args, **kwargs)
                end_time = time.time()
                print(f'[{pipeline}] [Collection]: Finished {message}...', round(end_time - start_time, 2))

                await db.pipeline_stats.add_collector_stats('data_collection', 'start_time', start_time)
                await db.pipeline_stats.add_collector_stats('data_collection', 'end_time', end_time)

                return collected_betting_lines

            return wrapper

        return decorator

    @staticmethod
    def processor_logger(pipeline, message: str):
        def decorator(processing_func):
            @functools.wraps(processing_func)
            def wrapped(self, *args, **kwargs):
                print(f'[{pipeline}] [Processing]: {message}...')
                result = processing_func(self, *args, **kwargs)
                print(f'[{pipeline}] [Processing]: Finished {message}...')

                db.pipeline_stats.add_processor_stats(self.times)

                return result

            return wrapped

        return decorator

    @staticmethod
    def pipeline_logger(message: str):
        def decorator(pipeline_func):
            @functools.wraps(pipeline_func)
            async def wrapped(self, *args, **kwargs):
                print(f'[{self.domain}]: {message}...')
                start_time = time.time()
                result = await pipeline_func(self, *args, **kwargs)
                end_time = time.time()
                print(f'[{self.domain}]: Finished {message}...')
                self.times['pipeline_time'] = round(end_time - start_time, 2)

                db.pipeline_stats.add_pipeline_stats(self.times)

                return result

            return wrapped

        return decorator