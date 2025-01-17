import functools
import time

from db import db


def logger(pipeline_func):
    @functools.wraps(pipeline_func)
    async def wrapped(self, *args, **kwargs):
        await self._configure_pipeline()
        while True:
            print(f'[{self.domain}Pipeline]: 🟢 Started Batch 🟢')
            start_time = time.time()
            await pipeline_func(self, *args, **kwargs)
            end_time = time.time()
            print(f'[{self.domain}Pipeline]: 🔴 Finished Batch 🔴 💤 {self.configs['throttle']} seconds 💤')

            self.times['pipeline_time'] = round(end_time - start_time, 2)
            db.pipeline_stats.add_pipeline_stats(self.times)

    return wrapped


class BasePipeline:

    def __init__(self, domain: str, configs: dict):
        self.domain = domain
        self.configs = configs

        self.times = {}

    def _configure_pipeline(self):
        raise NotImplementedError

    def run_pipeline(self):
        raise NotImplementedError


