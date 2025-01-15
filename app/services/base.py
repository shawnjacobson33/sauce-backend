from datetime import datetime

from app.services.utils import utilities as utils


class BasePipeline:

    def __init__(self, domain: str, configs: dict):
        self.domain = domain
        self.configs = configs

        self.times = {}

    def run_pipeline(self):
        raise NotImplementedError


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