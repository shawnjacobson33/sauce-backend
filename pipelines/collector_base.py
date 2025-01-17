import functools
import time
from datetime import datetime

from db import db
from pipelines.utils import utilities as utils


def logger(collection_func):
    @functools.wraps(collection_func)  # Preserves original function metadata
    async def wrapper(self, *args, **kwargs):
        print(f'[{self.domain}Pipeline] [Collection] [{self.name}]: 🟢 Started Collecting 🟢')
        start_time = time.time()
        await collection_func(self, *args, **kwargs)
        end_time = time.time()
        print((f'[{self.domain}Pipeline] [Collection] [{self.name}]: 🔴 Finished Collecting 🔴'
               f'⏳ {round(end_time - start_time, 2)} seconds ⏳'
               f'💰 {len(self.items_container)} {self.domain} 💰')
              )

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

    def log_error(self, e: Exception):
        print(f'[{self.domain}] [Collection] [{self.name}]: ⚠️', e, '⚠️')