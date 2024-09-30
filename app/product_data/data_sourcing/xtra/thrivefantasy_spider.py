import asyncio
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper


class ThriveFantasySpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='ThriveFantasy')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()
        cookies = self.helper.get_cookies()
        json_data = self.helper.get_json_data()
        await self.rm.post(url, self._parse_lines, headers=headers, cookies=cookies, json=json_data)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json().get('response')
        subject_ids = dict()
        leagues = set()
        for prop in data.get('data', []):
            contest_prop = prop.get('contestProp')
            # game info
            game_info = None
            admin_event = contest_prop.get('adminEvent')
            if admin_event:
                home_team, away_team = admin_event.get('homeTeam'), admin_event.get('awayTeam')
                if home_team and away_team:
                    game_info = ' @ '.join([away_team, home_team])

            # player info
            subject_id, player = None, contest_prop.get('player1')
            market_id, league, subject_team, position, subject, market = None, None, None, None, None, None
            if player:
                league = player.get('leagueType')
                if league:
                    league = DataCleaner.clean_league(league)
                    leagues.add(league)

                subject_team = player.get('teamAbbr')
                position = player.get('positionAbbreviation')
                # player name
                first_name, last_name = player.get('firstName'), player.get('lastName')
                if first_name and last_name:
                    subject = ' '.join([first_name, last_name])

                if subject:
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        cleaned_subject = DataCleaner.clean_subject(subject)
                        subject_id = self.dn.get_subject_id(cleaned_subject, league, subject_team)
                        subject_ids[f'{subject}{subject_team}'] = subject_id

                # market
                prop_params = player.get('propParameters')
                if prop_params and len(prop_params) > 1:
                    market = ' + '.join(prop_params)
                else:
                    market = prop_params[0]

                if market:
                    market_id = self.dn.get_market_id(market)

            line = contest_prop.get('propValue')
            for label in ['Over', 'Under']:
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'game_info': game_info,
                    'subject_team': subject_team,
                    'position': position,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': 'Thrive Fantasy',
                    'label': label,
                    'line': line
                })

        self.helper.store(self.prop_lines)
        print(leagues)


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = ThriveFantasySpider(uuid.uuid4(), RequestManager(), DataNormalizer('Thrive Fantasy', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[ThriveFantasy]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
