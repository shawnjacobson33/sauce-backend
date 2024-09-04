import os
import time
import uuid
from datetime import datetime, timedelta
import json

import pandas as pd
import asyncio
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import RequestManager
from pymongo.database import Database


class OddsShopperSpider:
    def __init__(self, batch_id: uuid.UUID, arm: RequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']

    async def start(self):
        url = f'https://www.oddsshopper.com/api/processingInfo/all'
        headers, cookies = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://www.oddsshopper.com/liveodds/mlb',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }, {
            '_gcl_au': '1.1.1623273209.1722653498',
            '_ga': 'GA1.1.578395929.1722653499',
            '_clck': 'sq3dhw%7C2%7Cfo0%7C0%7C1676',
            'advanced_ads_page_impressions': '%7B%22expires%22%3A2038059248%2C%22data%22%3A1%7D',
            'advanced_ads_browser_width': '1695',
            '_omappvp': '3R4fJFIbQqbzViRUPUXzhsn7uJbFjZ4ZycGfR5dL7cVUJS3AMw6TzfPLv0ZqsQycti3fiWi7kzDa8w4T9Tev789cLZG3Vzfq',
            '_hp2_id.2282576763': '%7B%22userId%22%3A%225144768874446519%22%2C%22pageviewId%22%3A%223326899841379432%22%2C%22sessionId%22%3A%221530461341951255%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
            '_hp2_ses_props.2436895921': '%7B%22ts%22%3A1722717777024%2C%22d%22%3A%22www.oddsshopper.com%22%2C%22h%22%3A%22%2Fliveodds%2Ftennis%22%7D',
            '_rdt_uuid': '1722653498619.4f5023ed-9e39-40e5-8323-7ed52f08d73c',
            '_clsk': 'ojlg6i%7C1722719433713%7C6%7C1%7Cw.clarity.ms%2Fcollect',
            '_ga_FF6BGPF4L9': 'GS1.1.1722717780.9.1.1722719608.0.0.0',
            '_ga_ZR0H6RP7T1': 'GS1.1.1722717778.9.1.1722719608.60.0.1437947439',
            '_hp2_id.2436895921': '%7B%22userId%22%3A%226206737379708108%22%2C%22pageviewId%22%3A%224229154290207751%22%2C%22sessionId%22%3A%228372167046692848%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
        }

        await self.arm.get(url, self._parse_processing_info, headers=headers)

    async def _parse_processing_info(self, response):
        data = response.json()

        # Now proceed to fetch the matchups using the lastProcessed time
        url = f'https://www.oddsshopper.com/api/liveOdds/offers?league=all'

        await self.arm.get(url, self._parse_matchups, data.get('lastProcessed'))

    async def _parse_matchups(self, response, last_processed):
        data = response.json()

        tasks = []
        for offer_category in data.get('offerCategories', []):
            market_category = offer_category.get('name')
            if market_category not in {'Gamelines', 'GameProps', 'PlayerFutures', 'TeamFutures', 'Futures'}:
                for offer in offer_category.get('offers', []):
                    offer_id, now = offer.get('id'), datetime.utcnow()
                    # Calculate current datetime and future date
                    start_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    end_date = (now + timedelta(days=8)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

                    url = f"https://api.oddsshopper.com/api/offers/{offer_id}/outcomes/live?"
                    headers, params = {
                        'Accept': '*/*',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Connection': 'keep-alive',
                        'Origin': 'https://www.oddsshopper.com',
                        'Referer': 'https://www.oddsshopper.com/',
                        'Sec-Fetch-Dest': 'empty',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Site': 'same-site',
                        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                        'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                    }, {
                        'state': 'NJ',
                        'startDate': start_date,
                        'endDate': end_date,
                        'edgeSportsbooks': 'Circa,FanDuel,Pinnacle',
                    }

                    tasks.append(
                        self.arm.get(url, self._parse_lines, last_processed, offer.get('leagueCode'),
                                     headers=headers, params=params))

        await asyncio.gather(*tasks)

        relative_path = '../data_samples/oddsshopper_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[OddsShopper]: {len(self.prop_lines)} lines')

    async def _parse_lines(self, response, last_processed, league):
        data = response.json()

        for event in data:
            game_time, market, subject = event.get('startDate'), event.get('offerName'), event.get('eventName')

            # different market names for the same market for NFL and NCAAF -- need to normalize
            # Define the market mappings
            market_mappings = {
                'Total Rushing + Receiving Yards': 'Total Rush + Rec Yards',
                'Total Passing + Rushing Yards': 'Total Pass + Rush Yards',
                'Total Passing + Rushing + Receiving TDs': 'Total Pass + Rush + Rec TDs',
            }

            # Check if the market exists in the mappings
            if market in market_mappings:
                market = market_mappings[market]
            elif 'Fantasy Score' in market:
                fantasy_score_mappings = {
                    'NBA': 'Basketball Fantasy Score',
                    'WNBA': 'Basketball Fantasy Score',
                    'NCAAB': 'Basketball Fantasy Score',
                    'NCAAF': 'Football Fantasy Score',
                    'NHL': 'Ice Hockey Fantasy Score',
                    'UFC': 'Fighting Fantasy Score',
                    'Tennis': 'Tennis Fantasy Score',
                }
                market = fantasy_score_mappings.get(league, market)
            elif market == 'Fantasy Points' and league == 'NFL':
                market = 'Football Fantasy Score'
            elif market == 'Hitter Fantasy Score' and league == 'MLB':
                market = 'Baseball Fantasy Score'

            market_id = ''
            if market:
                market_id = self.msc.find_one({'OddsShopper': market}, {'_id': 1})
                if market_id:
                    market_id = market_id.get('_id')

            for side in event.get('sides', []):
                label = side.get('label', subject)
                for outcome in side.get('outcomes', []):
                    true_win_prob = outcome.get('trueWinProbability')
                    odds, ev = outcome.get('odds'), outcome.get('ev')
                    prop_line = {
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'last_updated': last_processed,
                        'league': league,
                        'game_time': game_time,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market_name': market,
                        'subject': outcome.get('label', subject),
                        'bookmaker': outcome.get('sportsbookCode'),
                        'label': label,
                        'line': outcome.get('line', '0.5'),
                        'odds': round(odds, 3) if odds else None,
                        'true_win_prob': round(true_win_prob, 3) if true_win_prob else None,
                        'oddsshopper_ev': round(ev, 3) if ev else None
                    }

                    self.prop_lines.append(prop_line)


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = OddsShopperSpider(batch_id=uuid.uuid4(), arm=RequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[OddsShopper]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
