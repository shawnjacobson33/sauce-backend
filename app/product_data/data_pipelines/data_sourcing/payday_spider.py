import json
import os
import time
import uuid
from datetime import datetime

import asyncio
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner as dc

from app.product_data.data_pipelines.utils import RequestManager
from pymongo.database import Database


class PaydaySpider:
    def __init__(self, batch_id: uuid.UUID, arm: RequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']
        self.headers = {
            'Host': 'api.paydayfantasy.com',
            'accept': '*/*',
            'content-type': 'application/json',
            'user-agent': 'Betscoop/2 CFNetwork/1496.0.7 Darwin/23.5.0',
            'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5NDJmYWIwZi0zMTQzLTQ2NmQtOTQ3MS1jODBmYmM3ZGYzMGUiLCJqdGkiOiI1OTZiMjMwOTczMGJiYjEyNjliNWU2MGM5MmJjMWFmMmFmYTY5MWFmYmFiZWQ5ZWI0OTIwN2JkYjQ3Yjg5Nzg0M2U4ZjQyZTg1MzUyNmFhNCIsImlhdCI6MTcyNDA5NzY0MC44ODQ5MTgsIm5iZiI6MTcyNDA5NzY0MC44ODQ5MjcsImV4cCI6MTc1NTYzMzY0MC44NzU1MjUsInN1YiI6IjBiODI2Mzc3LTRkMTUtNDBlMC1iNjZiLTU4ZGI3YTQzOWU1MCIsInNjb3BlcyI6WyIqIl19.qvwpWkgMa25A1R35YdAwiH00hkKur8xyo70JJYALVWMb2ig-BUDRfXXWrOC-CW7t3Pgf1hx9uZQNrq0P5MtI6FgLyUBoEf-RprxvXNV3SPukk8W42M8DSJAmsmRMnc99nsDLJyGv85v1VdTXe_eF5B9lqCZIwyCiVhS3Sc-_lkTP7Hm0yAXFIMqrt6nmjhcloRwV-S0tdyr6V9DQhnxGPU-9NasolMNf41FeYLcxVaynID67XJFs3rqFBQUR6PLzQLAgV_3vmwVXLJWvIgdrASomYcOUvBuKu4eiAPnd6SL5KTVPfwtY8yk2TmafTZUw0YTDU6EBYNlloBqpV6UJ0Kw4MZTwyP0lOyfQznOQu8jSUGrXujtfHMOHDT9lpFNe4Xdl91aNz-JJ-K-tSZEQlcNxRNfDC3wuhYzW6Mg1oBj10pIl3rHF9HYjVccdxuHBE9hkgSk3VpqEfxV9px0sGY3M0-8kfoWuCHSk5VVI4hlpdganx1vFJgkuFRpGC5yQAFle8siVaAQ9k9I-C84tLewCSGDkNHyO1PU_yJ_ivBs6Zdj1ZasSw5pLT6PQmROWaq28Xx_iKxS_mTmQxSlVjEM4mxuWV-xfuvUX2tSf8GJw_91QoGpN-7hTckwXygnxaVwQ96lIu4LbXnwveWKbZZDINYv8d_uk5SAzzFX1pwU',
            'accept-language': 'en-US,en;q=0.9',
        }

    async def start(self):
        url = 'https://api.paydayfantasy.com/api/v2/app/contests/total'
        params = {
            'include_solo_contests': '1',
        }

        await self.arm.get(url, self._parse_leagues, headers=self.headers, params=params)

    async def _parse_leagues(self, response):
        data = response.json()

        url = 'https://api.paydayfantasy.com/api/v2/app/contests'

        tasks = []
        for league_data in data.get('data', []):
            league = league_data.get('slug')
            if league:
                params = {
                    'leagues': league,
                    'include_solo_contests': '1',
                }

                league = dc.clean_league(league)

                tasks.append(self.arm.get(url, self._parse_contests, league, headers=self.headers, params=params))

        await asyncio.gather(*tasks)

        relative_path = '../data_samples/payday_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[Payday]: {len(self.prop_lines)} lines')

    async def _parse_contests(self, response, league):
        data = response.json()

        contests = data.get('data', {}).get('contests')
        if contests:
            parlay_contest = contests[0]
            if 'Parlay Contest' in parlay_contest.get('name'):
                parlay_contest_id = parlay_contest.get('id')

                url = f'https://api.paydayfantasy.com/api/v2/app/contests/{parlay_contest_id}/games'

                await self.arm.get(url, self._parse_lines, league, headers=self.headers)

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
                market_id = ''
                market, line, player = player_prop.get('name'), player_prop.get('value'), player_prop.get('player')
                if market:
                    market_id = self.msc.find_one({'Payday': market}, {'_id': 1})
                    if market_id:
                        market_id = market_id.get('_id')

                if player:
                    subject_jersey_number = player.get('number')
                    subject, position = player.get('name'), player.get('position')

                    subject_team, team_id = '', player.get('team_id')
                    if team_id:
                        subject_team = teams_dict.get(team_id)

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
                            'subject_jersey_number': subject_jersey_number,
                            'subject': subject,
                            'bookmaker': 'Payday',
                            'label': label,
                            'line': line
                        })


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = PaydaySpider(batch_id=uuid.uuid4(), arm=RequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Payday]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
