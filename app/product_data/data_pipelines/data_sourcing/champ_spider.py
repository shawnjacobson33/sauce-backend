import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import RequestManager, DataStandardizer, DataCleaner, Helper, get_db, Subject


def get_leagues():
    return ['MLB', 'NFL', 'CFB', 'NBA', 'CBB', 'NHL']


class ChampSpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Champ')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()
        tasks = []
        for league in get_leagues():
            if league:
                if not Helper.is_league_good(DataCleaner.clean_league(league)):
                    continue

            json_data = self.helper.get_json_data(var=league)
            tasks.append(self.rm.post(url, self._parse_lines, league, headers=headers, json=json_data))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_lines(self, response, league):
        # get body content in json format
        data = response.json() if response.json() else {}
        if data:
            data = data.get('data', {}).get('readPicks', {})

        league = DataCleaner.clean_league(league)
        subject_ids = dict()
        for event in data.get('items', []):
            game_info = event.get('title')
            for player in event.get('competitors', []):
                subject_id, subject, position, subject_team, competitor = None, None, None, None, player.get('competitor')
                if competitor:
                    subject, position = competitor.get('longName'), competitor.get('position')
                    team = competitor.get('team')
                    if team:
                        subject_team = team.get('shortName')
                        if subject:
                            subject_id = subject_ids.get(f'{subject}{subject_team}')
                            if not subject_id:
                                cleaned_subj = DataCleaner.clean_subject(subject)
                                subject_obj = Subject(cleaned_subj, league, subject_team)
                                subject_id = self.ds.get_subject_id(subject_obj)
                                subject_ids[f'{subject}{subject_team}'] = subject_id
                        else:
                            continue

                for prop in player.get('props', []):
                    market, line = prop.get('title'), prop.get('value')
                    market_id = self.ds.get_market_id(market)
                    labels, multiplier, boost = ['Over', 'Under'], None, prop.get('boost')
                    if boost:
                        multiplier = boost.get('multiplier')
                        labels = ['Under'] if multiplier < 1 else ['Over']

                    for label in labels:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': market,
                            'subject_team': subject_team,
                            'subject_id': subject_id,
                            'subject': subject,
                            'position': position,
                            'bookmaker': 'Champ',
                            'label': label,
                            'line': line,
                            'multiplier': multiplier
                        })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = ChampSpider(batch_id, RequestManager(), DataStandardizer(batch_id, 'Champ', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Champ]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
