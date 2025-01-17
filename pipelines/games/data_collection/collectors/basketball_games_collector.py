import asyncio
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from urllib3.exceptions import ResponseError

from pipelines.collector_base import BaseCollector, logger
from pipelines.utils import utilities as utils


class BasketballGamesCollector(BaseCollector):

    def __init__(self, batch_timestamp: datetime, games_container: list, configs: dict):
        super().__init__('CBSSports', 'games', batch_timestamp, games_container, configs)

    async def _request_games(self, league: str):
        try:
            base_url = self.payload['urls'][league]['schedule']
            headers = self.payload['headers'][0]
            headers['referer'] = headers['referer'].format(league, 'schedule')
            cookies = self.payload['cookies']
            today = datetime.strptime(datetime.now().strftime('%Y%m%d'), '%Y%m%d')
            for i in range(self.payload['num_days_to_collect_from_league_map'][league]):
                date = today + timedelta(days=i)
                url = base_url.format(date.strftime('%Y%m%d'))
                if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
                    self.successful_requests += 1
                    self._parse_games(resp_html, league, date)

        except ResponseError as e:
            self.failed_requests += 1
            self.log_error(e)

    @staticmethod
    def _get_complete_date_time(date: datetime, time_str: str) -> datetime | str:
        if time_str[-1].lower() in ['a', 'p']:
            time_str = time_str.strip() + "m"  # Add 'm' to make it "am" or "pm"

        try:
            time_obj = datetime.strptime(time_str, "%I:%M %p")
            formatted_date_str = date.replace(hour=time_obj.hour, minute=time_obj.minute).strftime("%Y-%m-%dT%H:%M:%SZ")
            return datetime.strptime(formatted_date_str, "%Y-%m-%dT%H:%M:%SZ")

        except ValueError:
            return 'live'

    @staticmethod
    def _extract_team(span_elem) -> dict[str, str] | None:
        if a_elem := span_elem.find('a'):
            link_components = a_elem.get('href').split('/')
            if len(link_components) > 3:
                team = link_components[3]
                return team

    def _extract_game_time_and_box_score_url(self, row, date: datetime) -> tuple[str, datetime] | None:
        if game_time_div := row.find('div', {'class': 'CellGame'}):
           if a_elem := game_time_div.find('a'):
               box_score_url = a_elem.get('href').split('/')[-2]
               if 'scoreboard' not in box_score_url:
                   game_time = self._get_complete_date_time(date, a_elem.text)
                   return box_score_url, game_time

    # Todo: Implement team name standardization check to make sure we are using uniform team names
    def _parse_games(self, html: str, league: str, requested_date: datetime):
        soup = BeautifulSoup(html, 'html.parser')
        if divs := soup.find_all('div', {'class': 'TableBaseWrapper'}):
            if table := divs[-1].find('table', {'class': 'TableBase-table'}):
                if rows := table.find_all('tr'):
                    for row in rows[1:]:
                        if game_info := self._extract_game_time_and_box_score_url(row, requested_date):
                            box_score_url, game_time = game_info
                            if span_elems := row.find_all('span', {'class': 'TeamName'}):
                                if len(span_elems) > 1:
                                    if away_team := self._extract_team(span_elems[0]):
                                        if home_team := self._extract_team(span_elems[1]):
                                            game_dict = {
                                                '_id': box_score_url,
                                                'league': league,
                                                'away_team': away_team,
                                                'home_team': home_team,
                                            }
                                            if game_time == 'live': # Todo: Only a problem if you collect a game for the first time when its live
                                                game_dict['status'] = 'live'

                                            else:
                                                game_dict['game_time'] = game_time

                                            self.items_container.append(game_dict)


    @logger
    async def run_collector(self):
        tasks = []
        for league in self.configs['valid_leagues']:
            if utils.get_sport(league) == 'Basketball':
                tasks.append(self._request_games(league))

        await asyncio.gather(*tasks)
