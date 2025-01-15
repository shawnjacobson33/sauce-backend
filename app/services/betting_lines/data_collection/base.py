from datetime import datetime

from app.db import db
from app.services.base import BaseCollector
from app.services.configs import load_configs
from app.services.utils import Standardizer


class BaseBettingLinesCollector(BaseCollector):

    def __init__(self,
                 name: str,
                 batch_timestamp: datetime,
                 betting_lines_container: list[dict],
                 standardizer: Standardizer):

        super().__init__(name, 'BettingLines', batch_timestamp, betting_lines_container)
        self.standardizer = standardizer

        self.failed_subject_standardization = 0
        self.failed_market_standardization = 0

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
            'betting_lines_collected': self.num_collected
        }

    def run_collector(self):
        raise NotImplementedError


