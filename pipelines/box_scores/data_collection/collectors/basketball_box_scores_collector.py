import asyncio
from datetime import datetime
from typing import Iterable

from bs4 import BeautifulSoup, PageElement

from pipelines.base import BaseCollector, collector_logger
from pipelines.utils import utilities as utils
from pipelines.utils.standardization import Standardizer
from pipelines.utils.exceptions import RequestingError, StandardizationError


class BasketballBoxScoresCollector(BaseCollector):
    """
    A class to collect basketball box scores data.

    Attributes:
        standardizer (Standardizer): The standardizer for data.
        games (list[dict]): The list of games to collect box scores for.
    """

    def __init__(self,
                 batch_timestamp: datetime,
                 standardizer: Standardizer,
                 boxscores_container: list,
                 games: list[dict],
                 configs: dict):
        """
        Initializes the BasketballBoxScoresCollector with the given parameters.

        Args:
            batch_timestamp (datetime): The timestamp of the batch.
            standardizer (Standardizer): The standardizer for data.
            boxscores_container (list): The container to store box scores.
            games (list[dict]): The list of games to collect box scores for.
            configs (dict): The configuration settings.
        """
        super().__init__("CBSSports", 'BoxScores', batch_timestamp, boxscores_container, configs)
        self.standardizer = standardizer
        self.games = games

    def _get_payload_for_box_scores(self, league: str, game: dict) -> tuple[str, dict, dict]:
        """
        Gets the payload for the box scores request.

        Args:
            league (str): The league name.
            game (dict): The game data.

        Returns:
            tuple[str, dict, dict]: The URL, headers, and cookies for the request.
        """
        try:
            game_id = game['_id']
            mapped_league = self.payload['league_map'][league]
            url = self.payload['url'].format(mapped_league, game_id)
            headers = self.payload['headers'][0]
            headers['referer'] = url
            cookies = self.payload['cookies']
            return url, headers, cookies

        except Exception as e:
            self.log_message(message=f'Failed to get payload for box scores: {league} {game} {e}', level='EXCEPTION')

    async def _request_boxscores(self, league: str, game: dict):
        """
        Requests box scores data from the CBSSports API.

        Args:
            league (str): The league name.
            game (dict): The game data.
        """
        try:
            url, headers, cookies = self._get_payload_for_box_scores(league, game)
            if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
                self.successful_requests += 1
                self._parse_boxscores(resp_html, league, game)

        except RequestingError as e:
            self.log_message(message=f'Failed to request box scores: {game} {e}', level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(message=f'Failed to request box scores: {game} {e}', level='EXCEPTION')

    @staticmethod
    def _extract_period_time(soup: BeautifulSoup) -> str:
        try:
            period_time = soup.find('div', {'class': 'time'})
            period_time = period_time.text.strip()
            return period_time if period_time not in { 'Halftime', 'End', '4th', '2nd' } else '00:00'

        except Exception as e:
            raise Exception(f'Failed to extract period time: {e}')

    @staticmethod
    def _extract_period(soup: BeautifulSoup, league: str) -> str:
        try:
            period = soup.find('div', {'class': 'quarter'})
            period = period.text.strip()
            return period if period != 'End' else ( '4th' if league in { 'NBA' } else '2nd' )

        except Exception as e:
            raise Exception(f'Failed to extract period: {e}')

    @staticmethod
    def _extract_scores(soup: BeautifulSoup) -> dict[str, dict[str, int]]:
        try:
            scores = soup.find_all('div', {'class': 'score-text'})
            return {
                'away': { 'total': int(scores[0].text.strip()) },
                'home': { 'total': int(scores[1].text.strip()) }
            }

        except Exception as e:
            raise Exception(f'Failed to extract scores: {e}')

    def _extract_and_add_game_info(self, soup: BeautifulSoup, game: dict) -> None:
        """
        Extracts and adds game information from the HTML soup.

        Args:
            soup (BeautifulSoup): The HTML soup.
            game (dict): The game data.
        """
        try:
            if period_time := self._extract_period_time(soup):
                game['period_time'] = period_time

            if period := self._extract_period(soup, game['league']):
                game['period'] = period

            if scores := self._extract_scores(soup):
                game['scores'] = scores

        except Exception as e:
            self.log_message(message=f'Failed to extract game info: {game} {e}', level='EXCEPTION')

    def _extract_team(self, div) -> str | None:
        """
        Extracts the team name from the div element.

        Args:
            div (PageElement): The div element.

        Returns:
            str | None: The team name if found, otherwise None.
        """
        try:
            if (a_elem := div.find('a')) and (href := a_elem['href']):
                if team_name := href.split('/')[3]:
                    return team_name

        except Exception as e:
            self.log_message(message=f'Failed to extract team: {div} {e}', level='EXCEPTION')

    def _extract_position(self, span_elem) -> str:
        """
        Extracts the position from the span element.

        Args:
            span_elem (PageElement): The span element.

        Returns:
            str: The position.
        """
        try:
            position = span_elem.text.strip()
            return position

        except Exception as e:
            self.log_message(message=f'Failed to extract position: {span_elem} {e}', level='EXCEPTION')

    def _extract_raw_subject_name(self, cell) -> str | None:
        """
        Extracts the raw subject name from the cell element.

        Args:
            cell (PageElement): The cell element.

        Returns:
            str | None: The raw subject name if found, otherwise None.
        """
        try:
            a_elem = cell.find('a')
            href = a_elem['href']
            raw_subject_name = ' '.join(href.split('/')[-2].split('-'))
            return raw_subject_name

        except Exception as e:
            self.log_message(message=f'Failed to extract raw subject name: {cell} {e}', level='EXCEPTION')

    def _extract_subject(self, cell, league: str) -> dict[str, str] | None:
        """
        Extracts the subject from the cell element.

        Args:
            cell (PageElement): The cell element.
            league (str): The league name.

        Returns:
            dict[str, str] | None: The subject data if found, otherwise None.
        """
        try:
            raw_subject_name = self._extract_raw_subject_name(cell)
            cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
            subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
            std_subject_name = self.standardizer.standardize_subject_name(subject_key)
            return {
                'id': subject_key,
                'name': std_subject_name,
            }

        except StandardizationError as e:
            self.log_message(message=f'Failed to standardize subject: {league} {cell} {e}', level='WARNING')

        except Exception as e:
            self.log_message(message=f'Failed to extract subject: {league} {cell} {e}', level='EXCEPTION')

    def _extract_basketball_stats(self, cells, league: str) -> dict | None:
        """
        Extracts basketball stats from the cells.

        Args:
            cells (list[PageElement]): The list of cell elements.
            league (str): The league name.

        Returns:
            dict | None: The basketball stats if found, otherwise None.
        """
        try:
            box_score_data = [cell.text for cell in cells
                              if ('for-mobile' not in cell['class']) and ('hide-on-narrow' not in cell['class'])]
            if '-' not in box_score_data:
                box_score = {
                    'Points': int(box_score_data[0]),
                    'Rebounds': int(box_score_data[1]),
                    'Assists': int(box_score_data[2]),
                    'Field Goals Made': int(box_score_data[3].split('/')[0]),
                    'Field Goals Attempted': int(box_score_data[3].split('/')[1]),
                    '3-Pointers Made': int(box_score_data[4].split('/')[0]),
                    '3-Pointers Attempted': int(box_score_data[4].split('/')[1]),
                    'Free Throws Made': int(box_score_data[5].split('/')[0]),
                    'Free Throws Attempted': int(box_score_data[5].split('/')[1]),
                    'Personal Fouls': int(box_score_data[6]),
                    'Steals': int(box_score_data[7]),
                    'Blocks': int(box_score_data[8]),
                    'Turnovers': int(box_score_data[9]),
                }

                if league == 'NBA':
                    # college basketball doesn't have these stats on cbssports
                    box_score['Plus Minus'] = int(box_score_data[10][1:]) if '+' in box_score_data[10] else int(
                        box_score_data[10])
                    box_score['Fantasy Points'] = int(box_score_data[11])

                return box_score

        except Exception as e:
            self.log_message(message=f'Failed to extract basketball stats: {league} {cells} {e}', level='EXCEPTION')

    def _get_divs(self, soup: BeautifulSoup) -> Iterable:
        """
        Gets the div elements containing box scores from the HTML soup.

        Args:
            soup (BeautifulSoup): The HTML soup.

        Yields:
            PageElement: The div element containing box scores.
        """
        try:
            for box_score_div in soup.find_all('div', class_=['starters-stats', 'bench-stats']):
                yield box_score_div

        except Exception as e:
            self.log_message(message=f'Failed to get divs from soup: {e}', level='EXCEPTION')

    def _get_table(self, box_score_div):
        """
        Gets the table element from the box score div.

        Args:
            box_score_div (PageElement): The box score div element.

        Returns:
            PageElement: The table element.
        """
        try:
            return box_score_div.find('div', {'class': 'stats-viewable-area'}).find('table', {'class': 'stats-table'})

        except Exception as e:
            self.log_message(message=f'Failed to get table from box score div: {box_score_div} {e}', level='EXCEPTION')

    def _get_rows(self, box_score_div):
        """
        Gets the rows from the box score table.

        Args:
            box_score_div (PageElement): The box score div element.

        Yields:
            PageElement: The row element.
        """
        try:
            table = self._get_table(box_score_div)
            for row in [row for row in table.find_all('tr') if row['class'][0] not in {'header-row', 'total-row'}]:
                yield row

        except Exception as e:
            self.log_message(message=f'Failed to get rows from box score div: {box_score_div} {e}', level='EXCEPTION')

    def _get_cells(self, row):
        """
        Gets the cells from the row element.

        Args:
            row (PageElement): The row element.

        Returns:
            list[PageElement]: The list of cell elements.
        """
        try:
            return [cell for cell in row.find_all('td') if 'for-mobile' not in cell['class']]

        except Exception as e:
            self.log_message(message=f'Failed to get cells from row: {row} {e}', level='EXCEPTION')

    def _store_and_report_box_scores(self, box_score: dict, game: dict, league: str, subject: dict) -> None:
        try:
            self.items_container.append({
                '_id': f'{game['_id']}:{subject['id']}',
                'game': game,
                'league': league,
                'subject': subject,
                'box_score': box_score
            })

        except Exception as e:
            self.log_message(
                message=f'Failed to store and report box scores: {box_score} {game} {league} {subject} {e}',
                level='EXCEPTION')

    def _parse_boxscores(self, html: str, league: str, game: dict) -> None:
        """
        Parses the box scores from the HTML response.

        Args:
            html (str): The HTML response.
            league (str): The league name.
            game (dict): The game data.
        """
        soup = BeautifulSoup(html, 'html.parser')
        self._extract_and_add_game_info(soup, game)
        for box_score_div in self._get_divs(soup):
            for row in self._get_rows(box_score_div):
                if cells := self._get_cells(row):
                    if subject := self._extract_subject(cells[0], league):
                        if box_score := self._extract_basketball_stats(cells[1:], league):
                            self._store_and_report_box_scores(box_score, game, league, subject)

    @collector_logger
    async def run_collector(self):
        """
        Runs the collector to gather basketball box scores data.
        """
        tasks = []
        for league in self.configs['valid_leagues']:
            if utils.get_sport(league) == 'Basketball':
                for game in self.games:
                    if game['league'] == league:
                        tasks.append(self._request_boxscores(league, game))

        await asyncio.gather(*tasks)
        self.log_message(message=f'Collected {len(self.items_container)} basketball box scores', level='INFO')