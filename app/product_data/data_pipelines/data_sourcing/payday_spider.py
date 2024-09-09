import json
import os
import time
import uuid
from datetime import datetime

import asyncio
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper


class PaydaySpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Payday')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []
        self.headers = self.helper.get_headers()

    async def start(self):
        url = self.helper.get_url(name='leagues')
        params = self.helper.get_params(name='leagues')
        await self.rm.get(url, self._parse_leagues, headers=self.headers, params=params)

    async def _parse_leagues(self, response):
        data = response.json()
        url = self.helper.get_url(name='contests')
        tasks = []
        for league_data in data.get('data', []):
            league = league_data.get('slug')
            if league:
                params = self.helper.get_params(name='contests', var_1=league)
                league = DataCleaner.clean_league(league)
                tasks.append(self.rm.get(url, self._parse_contests, league, headers=self.headers, params=params))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_contests(self, response, league):
        data = response.json()
        contests = data.get('data', {}).get('contests')
        if contests:
            parlay_contest = contests[0]
            if 'Parlay Contest' in parlay_contest.get('name'):
                parlay_contest_id = parlay_contest.get('id')
                if parlay_contest_id:
                    url = self.helper.get_url().format(parlay_contest_id)
                    await self.rm.get(url, self._parse_lines, league, headers=self.headers)

    async def _parse_lines(self, response, league):
        data = response.json()
        for game in data.get('data', {}).get('games', []):
            game_info = game.get('title')
            # Need team ids and names for player info
            teams_dict, home_team, away_team = dict(), game.get('home_team'), game.get('away_team')
            if home_team and away_team:
                for team in [home_team, away_team]:
                    team_id, team_name = team.get('id'), team.get('code')
                    if team_id and team_name:
                        teams_dict[team_id] = team_name.upper()

            for player_prop in game.get('player_props', []):
                market_id = None
                market, line, player = player_prop.get('name'), player_prop.get('value'), player_prop.get('player')
                if market:
                    market_id = self.dn.get_market_id(market)

                if player:
                    subject_jersey_number = player.get('number')
                    subject, position = player.get('name'), player.get('position')
                    subject_id, subject_team, team_id = None, None, player.get('team_id')
                    if team_id:
                        subject_team = teams_dict.get(team_id)

                    if subject:
                        subject_id = self.dn.get_subject_id(subject, league, subject_team, position, subject_jersey_number)

                    for label in ['Over', 'Under']:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market_name': market,
                            'subject_team': subject_team,
                            'position': position,
                            'jersey_number': subject_jersey_number,
                            'subject_id': subject_id,
                            'subject': subject,
                            'bookmaker': 'Payday',
                            'label': label,
                            'line': line
                        })


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = PaydaySpider(uuid.uuid4(), RequestManager(), DataNormalizer('Payday', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Payday]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
