import time
import uuid
from datetime import datetime
import asyncio

from app.product_data.data_pipelines.utils import clean_subject, clean_league, RequestManager, DataStandardizer, \
    Helper, get_db, Market, Subject, clean_market


class OwnersBoxSpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='OwnersBox')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []
        self.headers = self.helper.get_headers()
        self.cookies = self.helper.get_cookies()

    async def start(self):
        url = self.helper.get_url(name='leagues')
        await self.rm.get(url, self._parse_leagues, headers=self.headers, cookies=self.cookies)

    async def _parse_leagues(self, response):
        data = response.json()
        url = self.helper.get_url(name='markets')
        tasks = []
        for league in data:
            if not Helper.is_league_good(clean_league(league)):
                continue

            params = self.helper.get_params(name='markets', var_1=league)
            tasks.append(self.rm.get(url, self._parse_markets, league, headers=self.headers, cookies=self.cookies, params=params))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_markets(self, response, league):
        data = response.json()
        url = self.helper.get_url()
        tasks = []
        for market in data:
            market_id = market.get('id')
            if not market_id:
                continue

            params = self.helper.get_params(var_1=league, var_2=market_id)
            tasks.append(self.rm.get(url, self._parse_lines, headers=self.headers, cookies=self.cookies, params=params))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()
        subject_ids = dict()
        for prop_line in data.get('markets', []):
            league = prop_line.get('sport')
            if league:
                league = clean_league(league)

            # get market
            market_id, market, market_type = None, None, prop_line.get('marketType')
            if market_type:
                market = market_type.get('name')
                if market:
                    market = clean_market(market)
                    market_id = self.ds.get_market_id(Market(market, league))

            # get game info
            game_info, game = None, prop_line.get('game')
            if game:
                away_team, home_team = game.get('awayTeam'), game.get('homeTeam')
                if away_team and home_team:
                    away_team_alias, home_team_alias = away_team.get('alias').upper(), home_team.get('alias').upper()
                    game_info = ' @ '.join([away_team_alias, home_team_alias])

            # get player
            subject_id, subject_team, position, subject, player = None, None, None, None, prop_line.get('player')
            if player:
                subject_team, position = player.get('teamAlias').upper(), player.get('position')
                first_name, last_name = player.get('firstName'), player.get('lastName')
                subject = clean_subject(' '.join([first_name, last_name]))
                subject_id = subject_ids.get(f'{subject}{subject_team}')
                if not subject_id:
                    subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                    subject_ids[f'{subject}{subject_team}'] = subject_id

            # get line
            line, balanced_line = 0, prop_line.get('line')
            if balanced_line:
                line = balanced_line.get('balancedLine')

            pick_options = prop_line.get('pickOptions')
            if pick_options:
                if ('MORE' in pick_options) and ('LESS' in pick_options):
                    for label in ['Over', 'Under']:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': market,
                            'subject_team': subject_team,
                            'position': position,
                            'subject_id': subject_id,
                            'subject': subject,
                            'bookmaker': 'OwnersBox',
                            'label': label,
                            'line': line
                        })
                else:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'league': league,
                        'game_info': game_info,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'subject_team': subject_team,
                        'position': position,
                        'subject_id': subject_id,
                        'subject': subject,
                        'bookmaker': 'OwnersBox',
                        'label': 'Over' if 'MORE' in pick_options else 'Under',
                        'line': line
                    })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = OwnersBoxSpider(batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[OwnersBox]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
