import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner as dc

from app.product_data.data_pipelines.utils import RequestManager
from pymongo.database import Database


class ParlayPlaySpider:
    def __init__(self, batch_id: uuid.UUID, arm: RequestManager, db: Database):
        self.prop_lines = []
        self.batch_id, self.arm = batch_id, arm

        self.msc, self.plc, self.sc = db['markets'], db['prop_lines'], db['subjects']

    async def start(self):
        url = 'https://parlayplay.io/api/v1/crossgame/search/?sport=All&league=&includeAlt=true'
        headers = {
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'X-ParlayPlay-Platform': 'web',
            'X-Parlay-Request': '1',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://parlayplay.io/?coupon=PP5&utm_campaign=ppc_brand&gclid=CjwKCAiA75itBhA6EiwAkho9e0WKfb9Q9CfMPQX3pItGGIyHcVBKOkTekc8rapMRzs75GX8hNshKcxoCUXsQAvD_BwE',
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': '1',
            'sec-ch-ua-platform': '"macOS"',
        }

        await self.arm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        data = response.json()
        last_updated = data.get('lastUpdated')
        if last_updated:
            last_updated = datetime.fromtimestamp(last_updated)

        for player in data.get('players', []):
            league, game_time, match = '', '', player.get('match')
            if match:
                player_league, game_time = match.get('league'), match.get('matchDate')
                if player_league:
                    league = player_league.get('leagueNameShort')
                    if league:
                        league = dc.clean_league(league)

            # get subject data
            subject, position, team, player_data = '', '', '', player.get('player')
            if player_data:
                subject, position = player_data.get('fullName'), player_data.get('position')
                team_data = player_data.get('team')
                if team_data:
                    team = team_data.get('teamAbbreviation')

            for stat in player.get('stats', []):
                is_boosted_payout, alt_lines = stat.get('isBoostedPayout'), stat.get('altLines')
                if alt_lines:
                    market = alt_lines.get('market')
                    if market == 'Player Fantasy Score':
                        if league == 'MLB':
                            market = 'Player Baseball Fantasy Score'
                        elif league in {'WNBA', 'NBA'}:
                            market = 'Player Basketball Fantasy Score'
                        elif league in {'NFL'}:
                            market = 'Player Football Fantasy Score'

                    market_id = self.msc.find_one({'ParlayPlay': market}, {'_id': 1})
                    if market_id:
                        market_id = market_id.get('_id')

                    for line in alt_lines.get('values', []):
                        stat_line = line.get('selectionPoints')
                        for label in ['Over', 'Under']:
                            odds = line.get('decimalPriceOver') if label == 'Over' else line.get('decimalPriceUnder')
                            if not odds:
                                continue

                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'game_time': game_time,
                                'market_category': 'player_props',
                                'market_id': market_id,
                                'market_name': market,
                                'subject_team': team,
                                'subject': subject,
                                'position': position,
                                'bookmaker': 'ParlayPlay',
                                'label': label,
                                'line': stat_line,
                                'odds': odds,
                                'is_boosted': is_boosted_payout
                            })

        relative_path = '../data_samples/parlayplay_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[ParlayPlay]: {len(self.prop_lines)} lines')


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = ParlayPlaySpider(batch_id=uuid.uuid4(), arm=RequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[ParlayPlay]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
