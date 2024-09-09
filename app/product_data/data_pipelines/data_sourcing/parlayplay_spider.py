import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper


class ParlayPlaySpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='ParlayPlay')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()
        await self.rm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        data = response.json()
        last_updated = data.get('lastUpdated')
        if last_updated:
            last_updated = datetime.fromtimestamp(last_updated)

        for player in data.get('players', []):
            league, game_time, match = None, None, player.get('match')
            if match:
                player_league, game_time = match.get('league'), match.get('matchDate')
                if player_league:
                    league = player_league.get('leagueNameShort')
                    if league:
                        league = DataCleaner.clean_league(league)

            # get subject data
            subject_id, subject, position, subject_team, player_data = None, None, None, None, player.get('player')
            if player_data:
                subject, position = player_data.get('fullName'), player_data.get('position')
                team_data = player_data.get('team')
                if team_data:
                    subject_team = team_data.get('teamAbbreviation')

                if subject:
                    subject_id = self.dn.get_subject_id(subject, league, subject_team, position)

            for stat in player.get('stats', []):
                is_boosted_payout, alt_lines = stat.get('isBoostedPayout'), stat.get('altLines')
                if alt_lines:
                    market, market_id = alt_lines.get('market'), None
                    if market == 'Player Fantasy Score':
                        if league == 'MLB':
                            market = 'Player Baseball Fantasy Score'
                        elif league in {'WNBA', 'NBA'}:
                            market = 'Player Basketball Fantasy Score'
                        elif league in {'NFL'}:
                            market = 'Player Football Fantasy Score'

                    if market:
                        market_id = self.dn.get_market_id(market)

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
                                'subject_team': subject_team,
                                'position': position,
                                'subject_id': subject_id,
                                'subject': subject,
                                'bookmaker': 'ParlayPlay',
                                'label': label,
                                'line': stat_line,
                                'odds': odds,
                                'is_boosted': is_boosted_payout
                            })

        self.helper.store(self.prop_lines)


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = ParlayPlaySpider(uuid.uuid4(), RequestManager(), DataNormalizer('ParlayPlay', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[ParlayPlay]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
