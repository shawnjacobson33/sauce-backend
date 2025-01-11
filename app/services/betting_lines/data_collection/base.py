from datetime import datetime

from app.services.configs import load_configs
from app.services.utils import utilities as utils, Standardizer


class BaseBettingLinesCollector:
    def __init__(self,
                 name: str,
                 batch_num: int,
                 batch_timestamp: datetime,
                 collected_betting_lines: list[dict],
                 standardizer: Standardizer):

        self.name = name
        self.batch_num = batch_num
        self.batch_timestamp = batch_timestamp
        self.collected_betting_lines = collected_betting_lines
        self.standardizer = standardizer

        self.configs = load_configs('general')
        self.payload = utils.requester.get_payload(domain='betting_lines', source_name=self.name)

        self.successful_requests = 0
        self.failed_requests = 0

        self.failed_subject_standardization = 0
        self.failed_market_standardization = 0

        self.betting_lines_collected = 0


    def get_stats(self) -> dict:
        return {
            'batch_num': self.batch_num,
            'batch_timestamp': self.batch_timestamp,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'betting_lines_collected': self.betting_lines_collected
        }

    def run_collector(self):
        raise NotImplementedError