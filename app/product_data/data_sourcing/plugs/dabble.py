import sys
import time
import uuid
from datetime import datetime
import asyncio

from app.product_data.data_sourcing.utils import clean_subject, clean_league, DataStandardizer, RequestManager, \
    Packager, get_db, Subject, Market, clean_market, Plug, Bookmaker, get_bookmaker


class Dabble(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []
        self.headers = self.packager.get_headers()
        self.cookies = self.packager.get_cookies()

    async def start(self):
        url = self.packager.get_url(name='competitions')
        await self.rm.get(url, self._parse_competitions, headers=self.headers, cookies=self.cookies)

    async def _parse_competitions(self, response):
        data = response.json().get('data')
        tasks = []
        for competition in data.get('activeCompetitions', []):
            competition_id, league = competition.get('id'), competition.get('displayName')
            if league:
                league = clean_league(league)
                if not Packager.is_league_good(league):
                    continue

            url = self.packager.get_url(name='events').format(competition_id)
            params = self.packager.get_params()
            tasks.append(self.rm.get(url, self._parse_events, league, params=params))

        await asyncio.gather(*tasks)
        self.packager.store(self.prop_lines)

    async def _parse_events(self, response, league):
        data = response.json()
        tasks = []
        for event in data.get('data', []):
            event_id, game_info, last_updated = event.get('id'), event.get('name'), event.get('updated')

            if event.get('isDisplayed'):
                url = self.packager.get_url().format(event_id)
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
            if market:
                market = clean_market(market)

            # don't want futures
            if 'Regular Season' in market:
                continue

            market_id, position = self.ds.get_market_id(Market(market, league)), player_prop.get('position')
            subject_id, subject, subject_team = None, player_prop.get('playerName'), player_prop.get('teamAbbreviation')
            if subject_team and league in {'NCAAF', 'NFL'}:
                subject_team = subject_team.upper()

            if subject:
                # Since the same subject has many prop lines it is much faster to keep a dictionary of subject ids
                # to avoid redundant queries.
                subject = clean_subject(subject)
                subject_id = subject_ids.get(f'{subject}{subject_team}')
                if not subject_id:
                    subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                    subject_ids[f'{subject}{subject_team}'] = subject_id

            label, line = player_prop.get('lineType').title(), player_prop.get('value')
            self.prop_lines.append({
                'batch_id': self.batch_id,
                'time_processed': datetime.now(),
                # 'last_updated': last_updated,
                'league': league,
                'game_info': game_info,
                'market_category': 'player_props',
                'market_id': market_id,
                'market': market,
                'subject_id': subject_id,
                'subject': subject,
                'bookmaker': self.info.name,
                'label': label,
                'line': line,
                'odds': self.info.default_payout.odds
            })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    bookmaker_info = Bookmaker(get_bookmaker(db, "Dabble"))
    spider = Dabble(bookmaker_info, batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Dabble]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    with open('log.txt', 'w') as f:
        sys.stdout = f
        asyncio.run(main())
