from datetime import datetime

from app.services.configs import load_configs
from app.services.utils import utilities as utils


class BasePipeline:

    def __init__(self, domain: str, reset: bool = False):
        self.domain = domain
        self.reset = reset

        self.times = {}

    def run_pipeline(self):
        raise NotImplementedError


class BaseCollector:
    
    def __init__(self, name: str, domain: str, batch_timestamp: datetime, items_container: list[dict]):
        self.name = name
        self.domain = domain
        self.batch_timestamp = batch_timestamp
        self.items_container = items_container

        self.configs = load_configs('general')
        self.payload = utils.requester.get_payload(self.name, domain=self.domain.lower())
        
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