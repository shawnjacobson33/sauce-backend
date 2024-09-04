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


class SleeperSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']
        self.headers = {
            'Host': 'api.sleeper.app',
            'x-amp-session': '1724697278937',
            'accept': 'application/json',
            'authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdmF0YXIiOiIxNWQ3Y2YyNTliYzMwZWFiOGY2MTIwZjQ1ZjY1MmZiNiIsImRpc3BsYXlfbmFtZSI6IlNoYXdudGhlcmVhbHNoYWR5IiwiZXhwIjoxNzU2MjMzMzEyLCJpYXQiOjE3MjQ2OTczMTIsImlzX2JvdCI6ZmFsc2UsImlzX21hc3RlciI6ZmFsc2UsInJlYWxfbmFtZSI6bnVsbCwidXNlcl9pZCI6NzI5MjAyODc1NTk4Nzc0MjcyLCJ2YWxpZF8yZmEiOiJwaG9uZSJ9.hvc8FXdweWwNkBvrhCJ8ytRcBkX5ilDZa77IQtgleJM',
            'x-api-client': 'api.cached',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Sleeper/93.1.0 CFNetwork/1496.0.7 Darwin/23.5.0',
            'x-device-id': '71009696-F347-40AA-AE8C-5247A63041DF',
            'x-platform': 'ios',
            'x-build': '93.1.0',
            'x-bundle': 'com.blitzstudios.sleeperbot',
        }

    async def start(self):
        url = 'https://api.sleeper.app/players'
        params = {
            'exclude_injury': 'false',
        }

        await self.arm.get(url, self._parse_players, headers=self.headers, params=params)

    async def _parse_players(self, response):
        data = response.json()

        # get players
        players = dict()
        for player in data:
            team = player.get('team')
            if not team:
                continue

            first_name, last_name, position = player.get('first_name'), player.get('last_name'), player.get('position')
            sport, player_id, subject = player.get('sport'), player.get('player_id'), ' '.join([first_name, last_name])
            if player_id and sport:
                if sport not in players:
                    players[sport] = {}

                players[sport][player_id] = {'team': team, 'player_name': subject, 'position': position}

        url = 'https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&eg=15.control'

        await self.arm.get(url, self._parse_lines, players, headers=self.headers)

    async def _parse_lines(self, response, players):
        data = response.json()

        for line in data:
            team, subject, position, player_id, league = '', '', '', line.get('subject_id'), line.get('sport')
            if league:
                league = dc.clean_league(league)

            if player_id:
                player = players.get(league).get(player_id)
                if player:
                    team, subject, position = player.get('team'), player.get('player_name'), player.get('position')

            market_id, last_updated, market = '', line.get('updated_at'), line.get('wager_type')
            if market:
                if market == 'fantasy_points':
                    if league == 'MLB':
                        market = 'baseball_fantasy_points'
                    elif league in {'NFL', 'NCAAF'}:
                        market = 'football_fantasy_points'
                    elif league in {'NBA', 'WNBA'}:
                        market = 'basketball_fantasy_points'

                market_id = self.msc.find_one({'Sleeper': market}, {'_id': 1})
                if market_id:
                    market_id = market_id.get('_id')

            if last_updated:
                # convert from unix to a datetime
                last_updated = datetime.fromtimestamp(last_updated / 1000)

            for option in line.get('options', []):
                label, line = option.get('outcome').title(), option.get('outcome_value')
                multiplier = option.get('payout_multiplier')

                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'last_updated': last_updated,
                    'league': league.upper(),
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market_name': market,
                    'subject_team': team,
                    'subject': subject,
                    'position': position,
                    'bookmaker': 'Sleeper',
                    'label': label,
                    'line': line,
                    'multiplier': multiplier
                })

        relative_path = '../data_samples/sleeper_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[Sleeper]: {len(self.prop_lines)} lines')


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = SleeperSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Sleeper]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
