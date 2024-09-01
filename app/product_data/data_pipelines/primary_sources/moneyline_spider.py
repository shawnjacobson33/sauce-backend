import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.request_management import AsyncRequestManager
from pymongo.collection import Collection


class MoneyLineSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, msc: Collection):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc = arm, msc

    async def start(self):
        url = 'https://moneylineapp.com/v3/API/v4/bets/all_available.php'
        headers, cookies, params = {
            'Host': 'moneylineapp.com',
            'Accept': 'application/json, text/plain, */*',
            'User-Agent': 'MoneyLine/1 CFNetwork/1496.0.7 Darwin/23.5.0',
            'Accept-Language': 'en-US,en;q=0.9',
        }, {
            'PHPSESSID': '3kndhe1s5enl10i7q25klikeek',
        }, {
            'userUUID': 'google-oauth2|111871382655879132434',
            'apiKey': '90c0720d-f666-4bb8-8af6-48221004028c',
        }

        await self.arm.get(url, self._parse_lines, headers=headers, cookies=cookies, params=params)

    async def _parse_lines(self, response):
        data = response.json()

        for bet in data.get('bets', []):
            subject, subject_team, subject_components = '', '', bet.get('title')
            if subject_components:
                subject_components = subject_components.split()
                team_components = subject_components[-1]
                subject, subject_team = ' '.join(subject_components[:-1]), team_components[1:-1]

            is_boosted, league, market = False, bet.get('league'), bet.get('bet_text')
            # don't want futures
            if 'Season' in market:
                continue
            if 'Discount' in market:
                is_boosted = True
                # removes the fluff from the name
                market_components = market.split(' (')
                if market_components:
                    market = market_components[0]

            # quick formatting adjustment
            if market in {'Hitter Fantasy Score', 'Pitcher Fantasy Score'}:
                market = 'Baseball Fantasy Points'

            market_id = self.msc.find_one({'MoneyLine': market}, {'_id': 1})
            if market_id:
                market_id = market_id.get('_id')

            for i in range(1, 3):
                label, line, option_components = '', '', bet.get(f'option_{i}')
                if option_components:
                    option_components = option_components.split()
                    if len(option_components) == 2:
                        label, line = option_components[0].lower().title(), option_components[1]

                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market_name': market,
                    'subject_team': subject_team,
                    'subject': subject,
                    'bookmaker': 'MoneyLine',
                    'label': label,
                    'line': line,
                    'is_boosted': is_boosted
                })

        relative_path = 'data_samples/moneyline_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[MoneyLine]: {len(self.prop_lines)} lines')


async def main():
    client = MongoClient('mongodb://localhost:27017/')

    db = client['sauce']

    spider = MoneyLineSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), msc=db['markets'])
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[MoneyLine]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
