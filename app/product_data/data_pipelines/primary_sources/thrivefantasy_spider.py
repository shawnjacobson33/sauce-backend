import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner as dc

from app.product_data.data_pipelines.utils.request_management import AsyncRequestManager
from pymongo.database import Database


class ThriveFantasySpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']

    async def start(self):
        url = 'https://api.thrivefantasy.com/houseProp/upcomingHouseProps'
        cookies, headers, json_data = {
            '__cf_bm': 'jbc_SuDVg18rKbg7nFWKqtJAEecibV2S9p2bSZXjuDw-1725385621-1.0.1.1-J03cu6bfUVwCpT97F1cM5rCFYfMOOPBeW1Gx74FXuZfpcyjD.hxqCIK9LQMPdU8fvah4_bPNba3LCBcRnTSmJw',
        }, {
            'Host': 'api.thrivefantasy.com',
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'x-tf-version': '1.1.39',
            'user-agent': 'ThriveFantasy/1 CFNetwork/1496.0.7 Darwin/23.5.0',
            'token': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0aGVyZWFsc2xpbSIsImF1ZGllbmNlIjoiSU9TIiwicGFzc3dvcmRMYXN0Q2hhbmdlZEF0IjpudWxsLCJpYXQiOjE3MjQwOTgwNDIsImV4cCI6MTcyNTgyNjA0Mn0.5NJJGuSmqGfPYYWwOWzr7t6BcJVvyZLgokvGgfbbCVOZz-rLjQYuWv2g_iQ-0A6mxTklflCH7Gv2XoMfrB8u6Q',
        }, {
            'half': 0,
            'currentPage': 1,
            'currentSize': 20,
        }

        await self.arm.post(url, self._parse_lines, headers=headers, cookies=cookies, json=json_data)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json().get('response')

        for prop in data.get('data', []):
            contest_prop = prop.get('contestProp')

            # game info
            game_info = ''
            admin_event = contest_prop.get('adminEvent')
            if admin_event:
                home_team, away_team = admin_event.get('homeTeam'), admin_event.get('awayTeam')
                if home_team and away_team:
                    game_info = ' @ '.join([away_team, home_team])

            # player info
            player = contest_prop.get('player1')
            market_id, league, team, position, subject, market = '', '', '', '', '', ''
            if player:
                league = player.get('leagueType')
                if league:
                    league = dc.clean_league(league)

                team = player.get('teamAbbr')
                position = player.get('positionAbbreviation')

                # player name
                first_name, last_name = player.get('firstName'), player.get('lastName')
                if first_name and last_name:
                    subject = ' '.join([first_name, last_name])

                # market
                prop_params = player.get('propParameters')
                if prop_params and len(prop_params) > 1:
                    market = ' + '.join(prop_params)
                else:
                    market = prop_params[0]

                market_id = self.msc.find_one({'ThriveFantasy': market}, {'_id': 1})
                if market_id:
                    market_id = market_id.get('_id')

            line = contest_prop.get('propValue')

            for label in ['Over', 'Under']:
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market_name': market,
                    'game_info': game_info,
                    'team': team,
                    'subject': subject,
                    'position': position,
                    'bookmaker': 'Thrive Fantasy',
                    'label': label,
                    'line': line
                })

        relative_path = '../data_samples/thrivefantasy_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[ThriveFantasy]: {len(self.prop_lines)} lines')


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = ThriveFantasySpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[ThriveFantasy]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
