import asyncio
from datetime import datetime, timedelta
from typing import Iterable

from bs4 import BeautifulSoup

from pipelines.utils import utilities as utils
from pipelines.utils.exceptions import RequestingError
from pipelines.base.base_collector import BaseCollector, collector_logger


class BasketballGamesCollector(BaseCollector):
    """
    A collector class for handling the data collection of basketball games.

    Inherits from:
        BaseCollector: The base class for collectors.
    """

    def __init__(self, batch_timestamp: datetime, games_container: list, configs: dict):
        """
        Initializes the BasketballGamesCollector with the given parameters.

        Args:
            batch_timestamp (datetime): The timestamp for the batch.
            games_container (list): The container to store collected games.
            configs (dict): The configuration settings.
        """
        super().__init__('CBSSports', 'Games', batch_timestamp, games_container, configs)

    def _get_payload_for_games(self, league: str) -> tuple[str, dict, dict]:
        """
        Retrieves the payload for requesting games data.

        Args:
            league (str): The league for which the payload is being retrieved.

        Returns:
            tuple: A tuple containing the base URL, headers, and cookies.

        Raises:
            Exception: If an error occurs while retrieving the payload.
        """
        try:
            base_url = self.payload['urls'][league]['schedule']
            headers = self.payload['headers'][0]
            headers['referer'] = headers['referer'].format(league, 'schedule')
            cookies = self.payload['cookies']

            return base_url, headers, cookies

        except Exception as e:
            self.log_message(message=f'Failed to get payload for games: {league} {e}', level='EXCEPTION')

    async def _request_games(self, league: str) -> None:
        """
        Requests games data for a given league.

        Args:
            league (str): The league for which games data is being requested.

        Raises:
            RequestingError: If an error occurs while requesting games data.
            Exception: If a general error occurs.
        """
        try:
            base_url, headers, cookies = self._get_payload_for_games(league)
            today = datetime.strptime(datetime.now().strftime('%Y%m%d'), '%Y%m%d')
            for i in range(
                    self.payload['num_days_to_collect_from_league_map'][league]):  # Todo: this should be configurable
                date = today + timedelta(days=i)
                url = base_url.format(date.strftime('%Y%m%d'))
                if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
                    self.successful_requests += 1
                    self._parse_games(resp_html, league, date)

        except RequestingError as e:
            self.failed_requests += 1
            self.log_message(message=f'Failed to request games: {league} {e}', level='ERROR')

        except Exception as e:
            self.log_message(message=f'Failed to request games: {league} {e}', level='EXCEPTION')

    def _get_complete_date_time(self, date: datetime, time_str: str) -> datetime | str:
        """
        Combines the date and time string into a complete datetime object.

        Args:
            date (datetime): The date.
            time_str (str): The time string.

        Returns:
            datetime | str: The complete datetime object or 'live' if the time is not valid.

        Raises:
            ValueError: If the time string is not in the correct format.
            Exception: If a general error occurs.
        """
        if time_str[-1].lower() in ['a', 'p']:
            time_str = time_str.strip() + "m"  # Add 'm' to make it "am" or "pm"

        try:
            time_obj = datetime.strptime(time_str, "%I:%M %p")
            formatted_date_str = date.replace(hour=time_obj.hour, minute=time_obj.minute).strftime("%Y-%m-%dT%H:%M:%SZ")
            return datetime.strptime(formatted_date_str, "%Y-%m-%dT%H:%M:%SZ")

        except ValueError:
            return 'live'

        except Exception as e:
            self.log_message(message=f'Failed to get complete date time: {time_str} {e}', level='EXCEPTION')

    def _get_rows(self, html: str):
        """
        Retrieves the rows of game data from the HTML content.

        Args:
            html (str): The HTML content.

        Returns:
            list: The list of rows containing game data.

        Raises:
            Exception: If an error occurs while retrieving the rows.
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            divs = soup.find_all('div', {'class': 'TableBaseWrapper'})
            table = divs[-1].find('table', {'class': 'TableBase-table'})
            rows = table.find_all('tr')
            return rows

        except Exception as e:
            self.log_message(message=f'Failed to get rows: {e}', level='EXCEPTION')

    def _extract_teams(self, row) -> Iterable:
        """
        Extracts the teams from a row of game data.

        Args:
            row: The row containing game data.

        Yields:
            str: The team name.

        Raises:
            Exception: If an error occurs while extracting the teams.
        """
        try:
            span_elems = row.find_all('span', {'class': 'TeamName'})
            for span_elem in span_elems[:2]:
                a_elem = span_elem.find('a')
                link_components = a_elem['href'].split('/')
                team = link_components[3]
                yield team

        except Exception as e:
            self.log_message(message=f'Failed to extract team: {e}', level='EXCEPTION')

    def _extract_game_time_and_box_score_url(self, row, date: datetime) -> tuple[str, datetime] | None:
        """
        Extracts the game time and box score URL from a row of game data.

        Args:
            row: The row containing game data.
            date (datetime): The date of the game.

        Returns:
            tuple | None: A tuple containing the box score URL and game time, or None if extraction fails.

        Raises:
            Exception: If an error occurs while extracting the game time and box score URL.
        """
        try:
            game_time_div = row.find('div', {'class': 'CellGame'})
            a_elem = game_time_div.find('a')
            box_score_url = a_elem['href'].split('/')[-2]
            if 'scoreboard' not in box_score_url:
                game_time = self._get_complete_date_time(date, a_elem.text)
                return box_score_url, game_time

        except Exception as e:
            self.log_message(message=f'Failed to extract game time and box score url: {row} {e}', level='EXCEPTION')

    def _store_game(self, box_score_url: str, league: str, teams: tuple, game_time: str) -> None:
        """
        Stores the game data in the container.

        Args:
            box_score_url (str): The box score URL.
            league (str): The league of the game.
            teams (tuple): The teams playing the game.
            game_time (str): The game time.

        Raises:
            Exception: If an error occurs while storing the game data.
        """
        try:
            game_dict = {
                '_id': box_score_url,
                'league': league,
                'away_team': teams[0],
                'home_team': teams[1],
            }
            if game_time == 'live':  # Todo: Only a problem if you collect a game for the first time when its live
                game_dict['status'] = 'live'
            else:
                game_dict['game_time'] = game_time

            self.items_container.append(game_dict)

        except Exception as e:
            self.log_message(message=f'Failed to store game: {e}', level='EXCEPTION')

    # Todo: Implement team name standardization check to make sure we are using uniform team names
    def _parse_games(self, html: str, league: str, requested_date: datetime):
        """
        Parses the games data from the HTML content.

        Args:
            html (str): The HTML content.
            league (str): The league of the games.
            requested_date (datetime): The date of the games.

        Raises:
            Exception: If an error occurs while parsing the games data.
        """
        if rows := self._get_rows(html):
            for row in rows[1:]:
                if game_info := self._extract_game_time_and_box_score_url(row, requested_date):
                    box_score_url, game_time = game_info
                    if teams := tuple(self._extract_teams(row)):
                        self._store_game(box_score_url, league, teams, game_time)

    @collector_logger
    async def run_collector(self):
        """
        Runs the collector to gather basketball games data.

        Raises:
            Exception: If an error occurs during the collector execution.
        """
        tasks = []
        try:
            for league in self.configs['valid_leagues']:
                if utils.get_sport(league) == 'Basketball':
                    tasks.append(self._request_games(league))

            await asyncio.gather(*tasks)
            self.log_message(message=f'Collected {len(self.items_container)} basketball games', level='INFO')

        except Exception as e:
            self.log_message(message=f'Failed to run collector: {e}', level='EXCEPTION')