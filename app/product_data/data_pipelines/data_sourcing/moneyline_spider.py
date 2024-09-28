import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import clean_subject, clean_league, RequestManager, DataStandardizer, \
    Helper, get_db, Subject, Market


class MoneyLineSpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='MoneyLine')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()
        cookies = self.helper.get_cookies()
        params = self.helper.get_params()
        await self.rm.get(url, self._parse_lines, headers=headers, cookies=cookies, params=params)

    async def _parse_lines(self, response):
        data = response.json()
        subject_ids = dict()
        for bet in data.get('bets', []):
            is_boosted, league, market = False, bet.get('league'), bet.get('bet_text')
            if league:
                league = clean_league(league)
                if not Helper.is_league_good(league):
                    continue

            # don't want futures
            if 'Season' in market:
                continue
            if 'Discount' in market:
                is_boosted = True
                # removes the fluff from the name
                market_components = market.split(' (')
                if market_components:
                    market = market_components[0]

            # quick formatting adjustment
            if market in {'Hitter Fantasy Score', 'Pitcher Fantasy Score', 'Hitter Fantasy Points', 'Pitcher Fantasy Points'}:
                market = 'Baseball Fantasy Points'
            elif market == 'Fantasy Points':
                if league in {'WNBA', 'NBA'}:
                    market = 'Basketball Fantasy Points'
                elif league in {'NFL', 'NCAAF'}:
                    market = 'Football Fantasy Points'

            market_id = self.ds.get_market_id(Market(market, league))
            subject_id, subject, subject_team, subject_components = None, None, None, bet.get('title')
            if subject_components:
                subject_components = subject_components.split()
                team_components = subject_components[-1]
                subject, subject_team = ' '.join(subject_components[:-1]), team_components[1:-1].replace('r.(', '')
                if subject:
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        cleaned_subj = clean_subject(subject)
                        subject_obj = Subject(cleaned_subj, league, subject_team)
                        subject_id = self.ds.get_subject_id(subject_obj)
                        subject_ids[f'{subject}{subject_team}'] = subject_id

            for i in range(1, 3):
                label, line, option_components = None, None, bet.get(f'option_{i}')
                if option_components:
                    option_components = option_components.split()
                    if len(option_components) == 2:
                        label, line = option_components[0].lower().title(), option_components[1]

                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_team': subject_team,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': 'MoneyLine',
                    'label': label,
                    'line': line,
                    'is_boosted': is_boosted
                })

        self.helper.store(self.prop_lines)


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = MoneyLineSpider(batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[MoneyLine]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
