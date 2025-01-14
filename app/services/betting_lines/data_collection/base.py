from datetime import datetime

from app.db import db
from app.services.configs import load_configs
from app.services.utils import utilities as utils, Standardizer


class BaseBettingLinesCollector:
    def __init__(self,
                 name: str,
                 batch_timestamp: datetime,
                 collected_betting_lines: list[dict],
                 standardizer: Standardizer):

        self.name = name
        self.batch_timestamp = batch_timestamp
        self.collected_betting_lines = collected_betting_lines
        self.standardizer = standardizer

        self.configs = load_configs('general')
        self.payload = utils.requester.get_payload(source_name=self.name)

        self.successful_requests = 0
        self.failed_requests = 0

        self.failed_subject_standardization = 0
        self.failed_market_standardization = 0

        self.num_betting_lines_collected = 0

    async def _get_game(self, league: str, subject_name: str) -> dict | None:
        try:
            if subject := await db.subjects.get_subject({'league': league, 'name': subject_name}, proj={'team': 1}):
                game = await db.games.get_game({
                    '$or': [
                        {'home_team': subject['team']['abbr_name']},
                        {'away_team': subject['team']['abbr_name']}
                    ]
                })
                return game

        except Exception as e:
            self.log_error(e)

    def get_stats(self) -> dict:
        return {  # Todo: log sizes of data in mem?
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'betting_lines_collected': self.num_betting_lines_collected
        }

    def run_collector(self):
        raise NotImplementedError

    def log_error(self, e: Exception):
        print(f'[BettingLines] [Collection] [{self.name}]: ⚠️', e, '⚠️')
