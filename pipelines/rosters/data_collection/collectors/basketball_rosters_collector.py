import asyncio
from datetime import datetime

from bs4 import BeautifulSoup
from urllib3.exceptions import ResponseError

from db import db
from pipelines.base import BaseCollector
from pipelines.utils import utilities as utils


class BasketballRostersCollector(BaseCollector):
    
    def __init__(self, batch_timestamp: datetime, rosters_container: list, configs: dict):
        super().__init__('CBSSports', 'rosters', batch_timestamp, rosters_container, configs)
        
    async def _request_rosters(self, league: str, team: dict) -> None:
        try:
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
                abbr_team_name = self.payload['ncaa_team_name_url_map'][league]['abbr_name'].get(abbr_team_name, abbr_team_name)
                full_team_name = self.payload['ncaa_team_name_url_map'][league]['full_name'].get(full_team_name, full_team_name)

            url = base_url.format(abbr_team_name, full_team_name)
            if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
                self._parse_rosters(league, team, resp_html)

        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1


    def _parse_rosters(self, league: str, team: dict, html: str) -> None:
        soup = BeautifulSoup(html, 'html.parser')
        if table := soup.find('table'):
            if (rows := table.find_all('tr')) and (len(rows) > 1):
                for row in rows[1:]:
                    if (cells := row.find_all('td')) and (len(cells) > 2):
                        if span_elem := cells[1].find('span', {'class': 'CellPlayerName--long'}):
                            if a_elem := span_elem.find('a'):
                                self.items_container.append({
                                    'league': league,
                                    'team': team,
                                    'position': cells[2].text.strip(),
                                    'name': a_elem.text.strip(),
                                    'jersey_number': cells[0].text.strip()
                                })

    @utils.logger.collector_logger(message='Running Collector')
    async def run_collector(self):
        tasks = []
        for league in self.configs['valid_leagues']:
            if utils.get_sport(league) == 'Basketball':
                teams = await db.teams.get_teams({'league': league if 'NCAA' not in league else 'NCAA'})  # Todo: fine for now...optimize in the future
                for team in teams:
                    tasks.append(self._request_rosters(league, team))
    
        await asyncio.gather(*tasks)
