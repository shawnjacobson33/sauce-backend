import asyncio
import functools
import time

from db import dev_db as db


def pipeline_logger(pipeline_func):

    @functools.wraps(pipeline_func)
    async def wrapped(self, *args, **kwargs):
        await self._configure_pipeline()
        while True:
            print(f'[{self.domain}Pipeline]: 🟢 Started Batch 🟢')
            start_time = time.time()
            await pipeline_func(self, *args, **kwargs)
            end_time = time.time()
            self.times['pipeline_time'] = round(end_time - start_time, 2)
            print(f'[{self.domain}Pipeline]: 🔴 Finished Batch 🔴\n'
                  f'--------> ⏱️ {self.times['pipeline_time']} seconds ⏱️\n'
                  f'--------> 💤 {self.configs['throttle']} seconds 💤')

            db.pipeline_stats.add_pipeline_stats(self.times)

            sleep_time = self.configs['throttle']
            await asyncio.sleep(sleep_time)

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

    def log_message(self, message: str, level: str = 'EXCEPTION'):
        level = level.lower()

        if level == 'info':
            print(f'[{self.domain}Pipeline]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[{self.domain}Pipeline]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[{self.domain}Pipeline]: ‼️', message, '‼️')

        if level == 'exception':
            print(f'[{self.domain}Pipeline]: ❌', message, '❌')
