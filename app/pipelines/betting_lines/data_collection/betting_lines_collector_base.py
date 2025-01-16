from datetime import datetime

from app.db import db
from app.pipelines.base import BaseCollector
from app.pipelines.utils import Standardizer


class BaseBettingLinesCollector(BaseCollector):

    def __init__(self,
                 name: str,
                 batch_timestamp: datetime,
                 betting_lines_container: list[dict],
                 standardizer: Standardizer,
                 configs: dict):

        super().__init__(name, 'betting_lines', batch_timestamp, betting_lines_container, configs)
        self.standardizer = standardizer

        self.failed_subject_standardization = 0
        self.failed_market_standardization = 0

    @staticmethod
    async def _get_subject(market_domain: str, league: str, subject_name: str) -> dict | None:
        if market_domain == 'PlayerProps':
            return await db.subjects.get_subject({'league': league, 'name': subject_name})
        elif market_domain == 'Gamelines':
            return await db.teams.get_team({'league': league, 'full_name': subject_name})

    @staticmethod
    def _get_team_name(market_domain: str, subject: dict) -> str:
        return subject['team']['abbr_name'] if market_domain == 'PlayerProps' else subject['abbr_name']

    async def _get_game(self, market_domain: str, league: str, subject_name: str) -> dict | None:
        try:
            if subject := await self._get_subject(market_domain, league, subject_name):
                team_name = self._get_team_name(market_domain, subject)
                game = await db.games.get_game({
                    '$or': [
                        {'league': subject['league'], 'home_team': team_name},
                        {'league': subject['league'], 'away_team': team_name}
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


