import functools

from app.db import db


def pipeline_logger(message: str):

    def decorator(pipeline_func):

        @functools.wraps(pipeline_func)
        def wrapped(self, *args, **kwargs):
            print(f'[BettingLinesPipeline]: {message}...')
            result = pipeline_func(self, *args, **kwargs)
            print(f'[BettingLinesPipeline]: Finished {message}...')

            db.pipeline_stats.add_pipeline_stats(self.times)

            return result

        return wrapped

    return decorator