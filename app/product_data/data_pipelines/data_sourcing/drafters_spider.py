import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import RequestManager, DataStandardizer, Helper, clean_subject, \
    get_db, Subject, Market, clean_market


class DraftersSpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Drafters')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()
        await self.rm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()
        subject_ids = dict()
        for event in data.get('entities', []):
            # do not want futures
            if 'Season' in event.get('_id'):
                continue

            if event:
                home_team, away_team = event.get('home'), event.get('away')
                if home_team and away_team:
                    subject_team, game_info = event.get('own'), ' @ '.join([away_team, home_team])
                    if subject_team == 'MMA':
                        continue

            for player in event.get('players', []):
                subject_id, position, subject = None, None, player.get('player_name')
                # get game info
                subject_team, game_info, event = None, None, player.get('event')
                position = player.get('player_position')
                if position:
                    position = position.strip()

                if subject:
                    subject = clean_subject(subject)
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        subject_id = self.ds.get_subject_id(Subject(subject, team=subject_team, position=position))
                        subject_ids[f'{subject}{subject_team}'] = subject_id

                market_id, market, line = None, player.get('bid_stats_name'), player.get('bid_stats_value')
                if market:
                    market = clean_market(market)
                    market_id = self.ds.get_market_id(Market(market))

                for label in ['Over', 'Under']:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'game_info': game_info,
                        'subject_team': subject_team,
                        'subject_id': subject_id,
                        'subject': subject,
                        'position': position,
                        'bookmaker': 'Drafters',
                        'label': label,
                        'line': line
                    })

        self.helper.store(self.prop_lines)


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = DraftersSpider(batch_id, RequestManager(), DataStandardizer(batch_id, db, has_grouping=False))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Drafters]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
