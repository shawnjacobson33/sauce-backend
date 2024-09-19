import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper, get_db


class ParlayPlaySpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_normalizer: DataNormalizer):
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

        subject_ids = dict()
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

                subject_id = subject_ids.get(f'{subject}{subject_team}')
                if not subject_id:
                    cleaned_subject = DataCleaner.clean_subject(subject)
                    subject_id = self.dn.get_subject_id(cleaned_subject, league, subject_team, position)
                    subject_ids[f'{subject}{subject_team}'] = subject_id

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
                                'market': market,
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
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = ParlayPlaySpider(batch_id, RequestManager(), DataNormalizer(batch_id, 'ParlayPlay', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[ParlayPlay]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
