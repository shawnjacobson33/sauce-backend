import json
import os
import time
import uuid
from datetime import datetime

from bs4 import BeautifulSoup
import asyncio

from app.product_data.data_pipelines.utils import DataCleaner as dc

from app.product_data.data_pipelines.utils.request_management import AsyncRequestManager
from pymongo import MongoClient
from pymongo.database import Database


class DraftKingsPick6:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm
        self.msc, self.plc = db['markets'], db['prop_lines']
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjU0NjgyNSIsImFwIjoiNjAxNDMxMzM3IiwiaWQiOiJmNmU0N2RjMzlkN2NjZDZkIiwidHIiOiI0NDM2MTQ5YjFhNzY3ZmRlMTg0MDZhZDE2ODAwY2YwMCIsInRpIjoxNzIzNjE1NzY3NzUzfX0=',
            'priority': 'u=1, i',
            'referer': 'https://pick6.draftkings.com/?sport=WNBA',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'traceparent': '00-4436149b1a767fde18406ad16800cf00-f6e47dc39d7ccd6d-01',
            'tracestate': '546825@nr=0-1-546825-601431337-f6e47dc39d7ccd6d----1723615767753',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }

    async def start(self):
        url = "https://pick6.draftkings.com/"

        await self.arm.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response):
        tasks, soup = [], BeautifulSoup(response.text, 'html.parser')
        for league in [sport_div.text for sport_div in soup.find_all('div', {'class': 'dkcss-7q5fzm'}) if
                       not sport_div.text.isnumeric()]:
            url = response.url + f"?sport={league}&_data=routes%2F_index"

            if league:
                league = dc.clean_league(league)

            tasks.append(self.arm.get(url, self._parse_lines, league, headers=self.headers))

        await asyncio.gather(*tasks)

        relative_path = '../data_samples/draftkingspick6_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[DraftKingsPick6]: {len(self.prop_lines)} lines')

    async def _parse_lines(self, response, league):
        text = response.text

        # needs cleaning
        def clean_json():
            first_index = text.find('data:{')
            return text[:first_index]

        data = json.loads(clean_json()).get('pickableIdToPickableMap')

        for pick in data.values():
            market_id, market, subject, subject_team, game_time, position, line = '', '', '', '', '', '', ''
            pickable, active_market = pick.get('pickable'), pick.get('activeMarket')
            if pickable:
                market_category = pickable.get('marketCategory')
                if market_category:
                    market = market_category.get('marketName')
                    market_id = self.msc.find_one({'DraftKings Pick6': market}, {'_id': 1})
                    if market_id:
                        market_id = market_id.get('_id')
                for entity in pickable.get('pickableEntities', []):
                    subject, competitions = entity.get('displayName'), entity.get('pickableCompetitions')
                    if competitions:
                        first_competition = competitions[0]
                        position, team_data = first_competition.get('positionName'), first_competition.get('team')
                        if team_data:
                            subject_team = team_data.get('abbreviation')

                        summary = first_competition.get('competitionSummary')
                        if summary:
                            game_time = summary.get('startTime')

            if active_market:
                line = active_market.get('targetValue')

            for label in ['Over', 'Under']:
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'game_time': game_time,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market_name': market,
                    'subject_team': subject_team,
                    'subject': subject,
                    'position': position,
                    'bookmaker': "DraftKings Pick6",
                    'label': label,
                    'line': line
                })


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = DraftKingsPick6(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[DraftKingsPick6]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
