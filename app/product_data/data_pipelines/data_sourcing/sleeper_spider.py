import asyncio
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, RequestManager, DataNormalizer, Helper


class SleeperSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Sleeper')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []
        self.headers = self.helper.get_headers()

    async def start(self):
        url = self.helper.get_url(name='players')
        params = self.helper.get_params()
        await self.rm.get(url, self._parse_players, headers=self.headers, params=params)

    async def _parse_players(self, response):
        data = response.json()
        # get players
        players = dict()
        for player in data:
            subject_team = player.get('team')
            if not subject_team:
                continue

            first_name, last_name, position = player.get('first_name'), player.get('last_name'), player.get('position')
            sport, player_id, subject = player.get('sport'), player.get('player_id'), ' '.join([first_name, last_name])
            if player_id and sport:
                if sport not in players:
                    players[sport] = {}

                players[sport][player_id] = {'subject_team': subject_team, 'player_name': subject, 'position': position}

        url = self.helper.get_url()
        await self.rm.get(url, self._parse_lines, players, headers=self.headers)

    async def _parse_lines(self, response, players):
        data = response.json()
        subject_ids = dict()
        for line in data:
            subject_team, subject, position, player_id, league = None, None, None, line.get('subject_id'), line.get('sport')
            if league:
                league = DataCleaner.clean_league(league)

            subject_id = None
            if player_id:
                player = players.get(league).get(player_id)
                if player:
                    subject_team, subject, position = player.get('subject_team'), player.get('player_name'), player.get('position')
                    if subject:
                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            subject_id = self.dn.get_subject_id(subject, league, subject_team, position)
                            subject_ids[f'{subject}{subject_team}'] = subject_id

            market_id, last_updated, market = None, line.get('updated_at'), line.get('wager_type')
            if market:
                if market == 'fantasy_points':
                    if league == 'MLB':
                        market = 'baseball_fantasy_points'
                    elif league in {'NFL', 'NCAAF'}:
                        market = 'football_fantasy_points'
                    elif league in {'NBA', 'WNBA'}:
                        market = 'basketball_fantasy_points'

                market_id = self.dn.get_market_id(market)

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
                    'market': market,
                    'subject_team': subject_team,
                    'position': position,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': 'Sleeper',
                    'label': label,
                    'line': line,
                    'multiplier': multiplier
                })

        self.helper.store(self.prop_lines)


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = SleeperSpider(uuid.uuid4(), RequestManager(), DataNormalizer('Sleeper', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Sleeper]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
