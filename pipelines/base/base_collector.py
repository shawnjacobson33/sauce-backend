import functools
import time
from datetime import datetime

from db import dev_db as db
from pipelines.utils import utilities as utils


# logger.add(
#     'logs/betting_lines_collection.log',
#     filter= lambda record: 'BettingLines' and 'Collection' in record['message'],
#     rotation='1 day',
#     level='INFO',
#     backtrace=True,
#     diagnose=True)
#
#
# logger.add(
#     'logs/box_scores_collection.log',
#     filter= lambda record: 'BoxScores' and 'Collection' in record['message'],
#     rotation='1 day',
#     level='INFO',
#     backtrace=True,
#     diagnose=True)


def collector_logger(collection_func):
    @functools.wraps(collection_func)  # Preserves original function metadata
    async def wrapper(self, *args, **kwargs):
        print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: 🟢 Started Collecting 🟢')
        start_time = time.time()
        await collection_func(self, *args, **kwargs)
        end_time = time.time()
        print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: 🔴 Finished Collecting 🔴')
        stats = self.get_stats()  # Todo: Consider switching to a 'self.times' dict for consistency
        db.pipeline_stats.add_collector_stats(self.name, stats)

        self.num_collected = 0

    return wrapper


class BaseCollector:

    def __init__(self, name: str, domain: str, batch_timestamp: datetime, items_container: list[dict], configs: dict):
        self.name = name
        self.domain = domain
        self.batch_timestamp = batch_timestamp
        self.items_container = items_container
        self.configs = configs

        self.payload = utils.requester.get_payload(self.name, domain=self.domain)

        self.successful_requests = 0
        self.failed_requests = 0

        self.num_collected = 0

    def run_collector(self, *args):
        raise NotImplementedError

    def get_stats(self):
        return {
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'num_collected': self.num_collected
        }

    def log_message(self, message: str, level: str = 'EXCEPTION'):
        level = level.lower()

        if level == 'info':
            print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: ❌', message, '❌')

        if level == 'exception':
            print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: ❌', message, '❌')
