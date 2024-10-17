import sys
import time
import uuid
from datetime import datetime, timedelta
import asyncio

from app.product_data.data_sourcing.utils import RequestManager, DataStandardizer, clean_market, clean_subject, \
    clean_league, Packager, get_db, Market, Subject, Plug, Bookmaker, get_bookmaker


class OddsShopper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []

    async def start(self):
        url = self.packager.get_url(name='processing_info')
        headers = self.packager.get_headers()
        cookies = self.packager.get_cookies()
        await self.rm.get(url, self._parse_processing_info, headers=headers, cookies=cookies)

    async def _parse_processing_info(self, response):
        data = response.json()
        # Now proceed to fetch the matchups using the lastProcessed time
        url = self.packager.get_url(name='matchups')
        await self.rm.get(url, self._parse_matchups, data.get('lastProcessed'))

    async def _parse_matchups(self, response, last_processed):
        data = response.json()
        tasks = []
        for offer_category in data.get('offerCategories', []):
            market_category = offer_category.get('name')
            if market_category == 'PlayerProps':
                for offer in offer_category.get('offers', []):
                    league = offer.get('leagueCode')
                    if league:
                        league = clean_league(league)
                        if not Packager.is_league_good(league):
                            continue

                    offer_id, now = offer.get('id'), datetime.utcnow()
                    # Calculate current datetime and future date
                    start_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    end_date = (now + timedelta(days=8)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    url = self.packager.get_url().format(offer_id)
                    headers = self.packager.get_headers()
                    params = self.packager.get_params(var_1=start_date, var_2=end_date)
                    tasks.append(
                        self.rm.get(url, self._parse_lines, last_processed, league,
                                     headers=headers, params=params)
                    )

        await asyncio.gather(*tasks)
        self.packager.store(self.prop_lines)

    async def _parse_lines(self, response, last_processed, league):
        data = response.json()
        subject_ids = dict()
        for event in data:
            # subject = event.get('eventName')
            game_time, market = event.get('startDate'), event.get('offerName')
            # different market names for the same market for NFL and NCAAF -- need to normalize
            # Define the market mappings
            market_mappings = {
                'Total Rushing + Receiving Yards': 'Total Rush + Rec Yards',
                'Total Passing + Rushing Yards': 'Total Pass + Rush Yards',
                'Total Passing + Rushing + Receiving TDs': 'Total Pass + Rush + Rec TDs',
            }
            # Check if the market exists in the mappings
            if market in market_mappings:
                market = market_mappings[market]
            elif 'Fantasy Score' in market:
                fantasy_score_mappings = {
                    'NBA': 'Basketball Fantasy Points',
                    'WNBA': 'Basketball Fantasy Points',
                    'NCAAB': 'Basketball Fantasy Points',
                    'NCAAF': 'Football Fantasy Points',
                    'NHL': 'Ice Hockey Fantasy Points',
                    'UFC': 'Fighting Fantasy Points',
                    'Tennis': 'Tennis Fantasy Points',
                }
                market = fantasy_score_mappings.get(league, market)
            elif market == 'Fantasy Points' and league == 'NFL':
                market = 'Football Fantasy Points'
            elif market == 'Hitter Fantasy Score' and league == 'MLB':
                market = 'Baseball Fantasy Points'

            market_id = None
            if market:
                market = clean_market(market)
                market_id = self.ds.get_market_id(Market(market, league))

            for side in event.get('sides', []):
                label = side.get('label')
                for outcome in side.get('outcomes', []):
                    subject, subject_id = outcome.get('label'), None
                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{league}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, league))
                            subject_ids[f'{subject}{league}'] = subject_id

                    true_win_prob = outcome.get('trueWinProbability')
                    odds, ev = outcome.get('odds'), outcome.get('ev')
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'last_updated': last_processed,
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'subject_id': subject_id,
                        'subject': outcome.get('label', subject),
                        'bookmaker': outcome.get('sportsbookCode'),
                        'label': label,
                        'line': outcome.get('line', '0.5'),
                        'odds': round(odds, 3) if odds else None,
                        'implied_prob': round(1 / odds, 3) if odds else None,
                        'true_win_prob': round(true_win_prob, 3) if true_win_prob else None,
                        'oddsshopper_ev': round(ev, 3) if ev else None
                    })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    bookmaker_info = Bookmaker(get_bookmaker(db, "OddsShopper"))
    spider = OddsShopper(bookmaker_info, batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[OddsShopper]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    with open('log.txt', 'w') as f:
        sys.stdout = f
        asyncio.run(main())
