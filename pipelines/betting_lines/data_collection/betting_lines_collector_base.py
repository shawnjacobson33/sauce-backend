from datetime import datetime

from db import db
from pipelines import utils
from pipelines.base.base_collector import BaseCollector
from pipelines.utils import Standardizer


class BaseBettingLinesCollector(BaseCollector):
    """
    A base class for collecting betting lines data.

    Attributes:
        standardizer (Standardizer): The standardizer for data.
        failed_subject_standardization (int): The count of failed subject standardizations.
        failed_market_standardization (int): The count of failed market standardizations.
    """

    def __init__(self,
                 name: str,
                 batch_timestamp: datetime,
                 betting_lines_container: list[dict],
                 standardizer: Standardizer,
                 configs: dict):
        """
        Initializes the BaseBettingLinesCollector with the given parameters.

        Args:
            name (str): The name of the collector.
            batch_timestamp (datetime): The timestamp of the batch.
            betting_lines_container (list[dict]): The container to store betting lines.
            standardizer (Standardizer): The standardizer for data.
            configs (dict): The configuration settings.
        """
        super().__init__(name, 'BettingLines', batch_timestamp, betting_lines_container, configs)
        self.standardizer = standardizer

        self.failed_subject_standardization = 0
        self.failed_market_standardization = 0

    @staticmethod
    async def _get_subject(market_domain: str, league: str, subject_name: str) -> dict | None:
        """
        Retrieves the subject based on the market domain, league, and subject name.

        Args:
            market_domain (str): The market domain (e.g., PlayerProps, Gamelines).
            league (str): The league name.
            subject_name (str): The subject name.

        Returns:
            dict | None: The subject data if found, otherwise None.
        """
        if market_domain == 'PlayerProps':
            return await db.subjects.get_subject({'league': league, 'name': subject_name})
        elif market_domain == 'Gamelines':
            return await db.teams.get_team({'league': league, 'full_name': subject_name})

    @staticmethod
    def _get_team_name(market_domain: str, subject: dict) -> str:
        """
        Retrieves the team name based on the market domain and subject data.

        Args:
            market_domain (str): The market domain (e.g., PlayerProps, Gamelines).
            subject (dict): The subject data.

        Returns:
            str: The team name.
        """
        return subject['team']['abbr_name'] if market_domain == 'PlayerProps' else subject['abbr_name']

    async def _get_game(self, market_domain: str, league: str, subject_name: str) -> dict | None:
        """
        Retrieves the game data based on the market domain, league, and subject name.

        Args:
            market_domain (str): The market domain (e.g., PlayerProps, Gamelines).
            league (str): The league name.
            subject_name (str): The subject name.

        Returns:
            dict | None: The game data if found, otherwise None.
        """
        if subject := await self._get_subject(market_domain, league, subject_name):
            try:
                team_name = self._get_team_name(market_domain, subject)
                if game := await db.games.get_game({
                    '$or': [
                        {'league': subject['league'], 'home_team': team_name},
                        {'league': subject['league'], 'away_team': team_name}
                    ]
                }, { 'league': 0 }):
                    game['game_time'] = game['game_time'].strftime('%Y-%m-%d %H:%M:%S')
                    return game

            except Exception as e:
                self.log_message(e, level='EXCEPTION')

    def _store_and_report(self, betting_line_dict: dict) -> None:
        """
        Stores the betting line data and updates the collection statistics.

        Args:
            betting_line_dict (dict): The betting line data.
        """
        try:
            betting_line_doc_key = utils.storer.get_betting_line_key(betting_line_dict)
            betting_line_dict['_id'] = betting_line_doc_key
            self.items_container.append(betting_line_dict)
            self.num_collected += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def get_stats(self) -> dict:
        """
        Retrieves the collection statistics.

        Returns:
            dict: The collection statistics.
        """
        return {
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'betting_lines_collected': self.num_collected
        }

    def run_collector(self):
        """
        Runs the collector to gather betting lines data.
        """
        raise NotImplementedError
