import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import RequestManager, DataNormalizer, DataCleaner, Helper

from pymongo import MongoClient


def get_leagues():
    return ['MLB', 'NFL', 'CFB', 'NBA', 'CBB', 'NHL']


class ChampSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Champ')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()

        tasks = []
        for league in get_leagues():
            json_data = self.helper.get_json_data(sport=league)
            tasks.append(self.rm.post(url, self._parse_lines, league, headers=headers, json=json_data))

        await asyncio.gather(*tasks)

        self.helper.store(self.prop_lines)

    async def _parse_lines(self, response, league):
        # get body content in json format
        data = response.json().get('data', {}).get('readPicks', {})

        if league:
            league = DataCleaner.clean_league(league)

        for event in data.get('items', []):
            game_info = event.get('title')
            for player in event.get('competitors', []):
                subject_id, subject, position, subject_team, competitor = '', '', '', '', player.get('competitor')
                if competitor:
                    subject, position = competitor.get('longName'), competitor.get('position')
                    team = competitor.get('team')
                    if team:
                        subject_team = team.get('shortName')
                        if subject:
                            cleaned_subject = DataCleaner.clean_subject(subject)
                            subject_id = self.dn.get_subject_id(cleaned_subject, league=league, subject_team=subject_team, position=position)
                        else:
                            continue

                for prop in player.get('props', []):
                    market, line = prop.get('title'), prop.get('value')
                    market_id = self.dn.get_market_id(market)

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
                            'market_name': market,
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
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = ChampSpider(uuid.uuid4(), RequestManager(), DataNormalizer('Champ', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Champ]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
