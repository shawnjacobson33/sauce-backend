import functools

from app.db import db


def processor_logger(message: str):

    def decorator(processing_func):

        @functools.wraps(processing_func)
        def wrapped(self, *args, **kwargs):
            print(f'[BettingLines] [Processing]: {message}...')
            result = processing_func(self, *args, **kwargs)
            print(f'[BettingLines] [Processing]: Finished {message}...')

            db.pipeline_stats.add_processor_stats(self.times)

            return result

        return wrapped

    return decorator