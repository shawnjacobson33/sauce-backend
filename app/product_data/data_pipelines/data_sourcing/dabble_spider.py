import time
import uuid
from datetime import datetime
import asyncio
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper


class DabbleSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Dabble')
        self.rm = request_manager
        self.dn = data_normalizer
        self.headers = self.helper.get_headers()
        self.cookies = self.helper.get_cookies()
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url(name='competitions')
        await self.rm.get(url, self._parse_competitions, headers=self.headers, cookies=self.cookies)

    async def _parse_competitions(self, response):
        data = response.json().get('data')
        tasks = []
        for competition in data.get('activeCompetitions', []):
            competition_id, league = competition.get('id'), competition.get('displayName')
            if league:
                league = DataCleaner.clean_league(league)

            url = self.helper.get_url(name='events').format(competition_id)
            params = self.helper.get_params()
            tasks.append(self.rm.get(url, self._parse_events, league, params=params))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_events(self, response, league):
        data = response.json()
        tasks = []
        for event in data.get('data', []):
            event_id, game_info, last_updated = event.get('id'), event.get('name'), event.get('updated')

            if event.get('isDisplayed'):
                url = self.helper.get_url().format(event_id)
                tasks.append(self.rm.get(url, self._parse_lines, league, game_info, last_updated))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league, game_info, last_updated):
        data = response.json().get('data')
        # get market groups
        markets = {
            market.get('id'): market_data.get('name')
            for market_data in data.get('marketGroupMappings', [])
            if market_data.get('name')  # Ensure market_name exists
            for market in market_data.get('markets', [])
            if market.get('id')  # Ensure market_id exists
        }

        subject_ids = dict()
        for player_prop in data.get('playerProps', []):
            market = markets.get(player_prop.get('marketId'))
            # don't want futures
            if 'Regular Season' in market:
                continue

            subject_id, market_id, position = None, self.dn.get_market_id(market), player_prop.get('position')
            subject, subject_team = player_prop.get('playerName'), player_prop.get('teamAbbreviation')
            if subject_team and league in {'NCAAF', 'NFL'}:
                subject_team = subject_team.upper()

            if subject:
                # Since the same subject has many prop lines it is much faster to keep a dictionary of subject ids
                # to avoid redundant queries.
                subject_id = subject_ids.get(f'{subject}{subject_team}')
                if not subject_id:
                    cleaned_subject = DataCleaner.clean_subject(subject)
                    subject_id = self.dn.get_subject_id(cleaned_subject, league=league, subject_team=subject_team, position=position)
                    subject_ids[f'{subject}{subject_team}'] = subject_id

            label, line = player_prop.get('lineType').title(), player_prop.get('value')
            self.prop_lines.append({
                'batch_id': self.batch_id,
                'time_processed': datetime.now(),
                'last_updated': last_updated,
                'league': league,
                'game_info': game_info,
                'market_category': 'player_props',
                'market_id': market_id,
                'market_name': market,
                'subject_team': subject_team,
                'subject_id': subject_id,
                'subject': subject,
                'position': position,
                'bookmaker': 'Dabble',
                'label': label,
                'line': line
            })


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = DabbleSpider(uuid.uuid4(), RequestManager(), DataNormalizer('Dabble', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Dabble]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
