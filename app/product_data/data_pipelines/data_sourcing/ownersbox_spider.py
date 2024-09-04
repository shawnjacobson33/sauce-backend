import json
import os
import time
import uuid
from datetime import datetime

import asyncio
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner as dc

from app.product_data.data_pipelines.utils.request_management import AsyncRequestManager
from pymongo.database import Database


class OwnersBoxSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']
        self.headers, self.cookies = {
            'Host': 'app.ownersbox.com',
            'accept': 'application/json',
            'content-type': 'application/json',
            'user-agent': 'OwnersBox/145 CFNetwork/1496.0.7 Darwin/23.5.0',
            'ownersbox_version': '7.12.0',
            'accept-language': 'en-US,en;q=0.9',
            'ownersbox_device': 'ios',
        }, {
            'obauth': 'eyJhbGciOiJIUzUxMiJ9.eyJvYnRva2VuIjoiMFE2SzJRMVkxTjhEIiwidXNlclN0YXRlIjoiQUNUSVZFIiwiaXNzIjoiT3duZXJzQm94IiwidmVyaWZpZWQiOmZhbHNlLCJ0b2tlbkV4cGlyeSI6MTcyNDEyMDAxMjc4MCwic2Vzc2lvbklkIjozNDE5MDU1MDk2fQ.9dcOi9DJ8_R1PTD4-m3VqXebAj1pZ0LAFzaXsaIGkIsvrjLOjF9jW5KNQEoDYOKjLhyzahtGd7VObdR1ABwNUA',
        }

    async def start(self):
        url = 'https://app.ownersbox.com/fsp-marketing/getSportInfo'

        await self.arm.get(url, self._parse_leagues, headers=self.headers, cookies=self.cookies)

    async def _parse_leagues(self, response):
        data = response.json()

        url = 'https://app.ownersbox.com/fsp/marketType/active?'

        tasks = []
        for league in data:
            params = {
                'sport': league,
            }

            tasks.append(self.arm.get(url, self._parse_markets, league, headers=self.headers, cookies=self.cookies, params=params))

        await asyncio.gather(*tasks)

        relative_path = '../data_samples/ownersbox_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[OwnersBox]: {len(self.prop_lines)} lines')

    async def _parse_markets(self, response, league):
        data = response.json()

        url = 'https://app.ownersbox.com/fsp/v2/market?'

        tasks = []
        for market in data:
            market_id = market.get('id')
            if not market_id:
                continue

            params = {
                'sport': league,
                'marketTypeId': market_id
            }

            tasks.append(self.arm.get(url, self._parse_lines, headers=self.headers, cookies=self.cookies, params=params))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()

        for prop_line in data.get('markets', []):
            league = prop_line.get('sport')

            if league:
                dc.clean_league(league)

            # get market
            market_id, market, market_type = '', '', prop_line.get('marketType')
            if market_type:
                market = market_type.get('name')
                market_id = self.msc.find_one({'OwnersBox': market}, {'_id': 1})
                if market_id:
                    market_id = market_id.get('_id')

            # get game info
            game_info, game = '', prop_line.get('game')
            if game:
                away_team, home_team = game.get('awayTeam'), game.get('homeTeam')
                if away_team and home_team:
                    away_team_alias, home_team_alias = away_team.get('alias').upper(), home_team.get('alias').upper()
                    game_info = ' @ '.join([away_team_alias, home_team_alias])

            # get player
            team, position, subject, player = '', '', '', prop_line.get('player')
            if player:
                team, position = player.get('teamAlias').upper(), player.get('position')
                first_name, last_name = player.get('firstName'), player.get('lastName')
                subject = ' '.join([first_name, last_name])

            # get line
            line, balanced_line = 0, prop_line.get('line')
            if balanced_line:
                line = balanced_line.get('balancedLine')

            pick_options = prop_line.get('pickOptions')
            if pick_options:
                if ('MORE' in pick_options) and ('LESS' in pick_options):
                    for label in ['Over', 'Under']:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market_name': market,
                            'subject_team': team,
                            'position': position,
                            'subject': subject,
                            'bookmaker': 'OwnersBox',
                            'label': label,
                            'line': line
                        })
                else:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'league': league,
                        'game_info': game_info,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market_name': market,
                        'subject_team': team,
                        'position': position,
                        'subject': subject,
                        'bookmaker': 'OwnersBox',
                        'label': 'Over' if 'MORE' in pick_options else 'Under',
                        'line': line
                    })


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = OwnersBoxSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[OwnersBox]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
