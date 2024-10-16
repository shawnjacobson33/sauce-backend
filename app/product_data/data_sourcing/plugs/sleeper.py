import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_sourcing.utils import clean_subject, clean_market, clean_league, RequestManager, \
    DataStandardizer, Packager, get_db, Subject, Market, Plug, Bookmaker, get_bookmaker


class Sleeper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []
        self.headers = self.packager.get_headers()

    async def start(self):
        url = self.packager.get_url(name='players')
        params = self.packager.get_params()
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

        url = self.packager.get_url()
        await self.rm.get(url, self._parse_lines, players, headers=self.headers)

    async def _parse_lines(self, response, players):
        data = response.json()
        subject_ids = dict()
        for line in data:
            subject_team, subject, position, player_id, league = None, None, None, line.get('subject_id'), line.get(
                'sport')
            cleaned_league = None
            if league:
                cleaned_league = clean_league(league)
                if not Packager.is_league_good(cleaned_league):
                    continue

            subject_id = None
            if player_id:
                player = players.get(league).get(player_id)
                if player:
                    subject_team, subject, position = player.get('subject_team'), player.get('player_name'), player.get(
                        'position')
                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, cleaned_league, subject_team, position))
                            subject_ids[f'{subject}{subject_team}'] = subject_id

            market_id, last_updated, market = None, line.get('updated_at'), line.get('wager_type')
            if market:
                if market == 'fantasy_points':
                    if cleaned_league == 'MLB':
                        market = 'Baseball Fantasy Points'
                    elif cleaned_league in {'NFL', 'NCAAF'}:
                        market = 'Football Fantasy Points'
                    elif cleaned_league in {'NBA', 'WNBA'}:
                        market = 'Basketball Fantasy Points'

                market = clean_market(market)
                market_id = self.ds.get_market_id(Market(market, cleaned_league))

            if last_updated:
                # convert from unix to a datetime
                last_updated = datetime.fromtimestamp(last_updated / 1000)

            for option in line.get('options', []):
                label, line = option.get('outcome').title(), option.get('outcome_value')
                odds = option.get('payout_multiplier')
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    # 'last_updated': last_updated,
                    'league': cleaned_league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': self.info.name,
                    'label': label,
                    'line': line,
                    'odds': odds
                })

        self.packager.store(self.prop_lines)


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    bookmaker_info = Bookmaker(get_bookmaker(db, "Sleeper"))
    spider = Sleeper(bookmaker_info, batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Sleeper]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
