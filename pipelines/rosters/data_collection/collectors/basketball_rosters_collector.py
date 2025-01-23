import asyncio
from datetime import datetime

from bs4 import BeautifulSoup

from db import db
from pipelines.base.base_collector import BaseCollector, collector_logger
from pipelines.utils import utilities as utils
from pipelines.utils.exceptions import RequestingError


class BasketballRostersCollector(BaseCollector):
    """
    A collector class for handling basketball rosters data.

    Inherits from:
        BaseCollector: The base class for collecting data.
    """

    def __init__(self, batch_timestamp: datetime, rosters_container: list, configs: dict):
        """
        Initializes the BasketballRostersCollector with the given parameters.

        Args:
            batch_timestamp (datetime): The timestamp for the batch.
            rosters_container (list): The container for rosters data.
            configs (dict): The configuration settings.
        """
        super().__init__('CBSSports', 'Rosters', batch_timestamp, rosters_container, configs)

    def _get_payload_for_rosters(self, league: str, team: dict) -> tuple[str, dict, dict]:
        """
        Constructs the payload for requesting rosters.

        Args:
            league (str): The league name.
            team (dict): The team information.

        Returns:
            tuple[str, dict, dict]: The URL, headers, and cookies for the request.
        """
        base_url = self.payload['urls'][league]['rosters']
        headers = self.payload['headers'][0]
        headers['referer'] = headers['referer'].format(league, 'teams')
        cookies = self.payload['cookies']
        abbr_team_name = team['abbr_name']
        full_team_name = ('-'.join(team['full_name'].lower().split())
                          .replace('.', '')
                          .replace('(', '')
                          .replace(')', '')
                          .replace("'", ''))
        if 'NCAA' in league:
            abbr_team_name = self.payload['ncaa_team_name_url_map'][league]['abbr_name'].get(abbr_team_name,
                                                                                             abbr_team_name)
            full_team_name = self.payload['ncaa_team_name_url_map'][league]['full_name'].get(full_team_name,
                                                                                             full_team_name)

        url = base_url.format(abbr_team_name, full_team_name)

        return url, headers, cookies

    async def _request_rosters(self, league: str, team: dict) -> None:
        """
        Requests the rosters for a given league and team.

        Args:
            league (str): The league name.
            team (dict): The team information.

        Raises:
            RequestingError: If an error occurs during the request.
        """
        try:
            url, headers, cookies = self._get_payload_for_rosters(league, team)
            if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
                self._parse_rosters(league, team, resp_html)

        except RequestingError as e:
            self.log_message(e, level='EXCEPTION')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _get_rows(self, soup) -> list | None:
        """
        Extracts the rows from the HTML soup.

        Args:
            soup (BeautifulSoup): The parsed HTML soup.

        Returns:
            list | None: The list of rows or None if an error occurs.
        """
        try:
            table = soup.find('table')
            rows = table.find_all('tr')
            return rows

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_position(self, cells: list) -> str:
        """
        Extracts the position from the table cells.

        Args:
            cells (list): The list of table cells.

        Returns:
            str: The extracted position.
        """
        try:
            return cells[2].text.strip()

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_name(self, cells: list) -> str:
        """
        Extracts the name from the table cells.

        Args:
            cells (list): The list of table cells.

        Returns:
            str: The extracted name.
        """
        try:
            if span_elem := cells[1].find('span', {'class': 'CellPlayerName--long'}):
                if a_elem := span_elem.find('a'):
                    return a_elem.text.strip()

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_jersey_number(self, cells: list) -> str:
        """
        Extracts the jersey number from the table cells.

        Args:
            cells (list): The list of table cells.

        Returns:
            str: The extracted jersey number.
        """
        try:
            return cells[0].text.strip()

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _parse_rosters(self, league: str, team: dict, html: str) -> None:
        """
        Parses the rosters from the HTML content.

        Args:
            league (str): The league name.
            team (dict): The team information.
            html (str): The HTML content.
        """
        soup = BeautifulSoup(html, 'html.parser')
        if rows := self._get_rows(soup):
            for row in rows[1:]:
                if cells := row.find_all('td'):
                    if position := self._extract_position(cells):
                        if name := self._extract_name(cells):
                            if jersey_number := self._extract_jersey_number(cells):
                                self.items_container.append({
                                    'league': league,
                                    'team': team,
                                    'position': position,
                                    'name': name,
                                    'jersey_number': jersey_number
                                })

    @collector_logger
    async def run_collector(self):
        """
        Runs the collector to gather rosters data.

        Raises:
            Exception: If an error occurs during the collection process.
        """
        try:
            tasks = []
            for league in self.configs['valid_leagues']:
                if utils.get_sport(league) == 'Basketball':
                    teams = await db.teams.get_teams({'league': league if 'NCAA' not in league else 'NCAA'})  # Todo: fine for now...optimize in the future
                    for team in teams:
                        tasks.append(self._request_rosters(league, team))

            await asyncio.gather(*tasks)

        except Exception as e:
            self.log_message(e, level='EXCEPTION')