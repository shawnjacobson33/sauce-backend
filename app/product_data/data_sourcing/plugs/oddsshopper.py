from datetime import datetime, timedelta
import asyncio

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.constants import FANTASY_SCORE_MAP
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league


# Different market names for the same market for NFL and NCAAF -- need to normalize
MARKET_MAP = {
    'Total Rushing + Receiving Yards': 'Total Rush + Rec Yards',
    'Total Passing + Rushing Yards': 'Total Pass + Rush Yards',
    'Total Passing + Rushing + Receiving TDs': 'Total Pass + Rush + Rec TDs',
}


class OddsShopper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url(name='matchups')
        headers = self.packager.get_headers()
        cookies = self.packager.get_cookies()
        await self.rm.get(url, self._parse_matchups, headers=headers, cookies=cookies)

    async def _parse_matchups(self, response):
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

                    offer_id, now = offer.get('id'), datetime.now()
                    # Calculate current datetime and future date
                    start_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    end_date = (now + timedelta(days=8)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    url = self.packager.get_url().format(offer_id)
                    headers = self.packager.get_headers()
                    params = self.packager.get_params(var_1=start_date, var_2=end_date)
                    tasks.append(self.rm.get(url, self._parse_lines, league, headers=headers, params=params))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league):
        data = response.json()
        subject_ids = dict()
        for event in data:
            # subject = event.get('eventName')
            market_id, game_time, market = None, event.get('startDate'), event.get('offerName')
            if market:
                # Check if the market exists in the mappings
                if market in MARKET_MAP:
                    market = MARKET_MAP[market]
                elif ('Fantasy Score' in market) or (market == 'Fantasy Points'):
                    market = FANTASY_SCORE_MAP.get(league, market)

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
                            subject_id = self.ds.get_subject_id(Subject(subject, league), user='OddsShopper')
                            subject_ids[f'{subject}{league}'] = subject_id

                    bookmaker = outcome.get('sportsbookCode')
                    true_win_prob = outcome.get('trueWinProbability')
                    odds, ev = outcome.get('odds'), outcome.get('ev')
                    # update shared data
                    PropLines.update(''.join(bookmaker.split()).lower(), {
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'subject_id': subject_id,
                        'subject': subject,
                        'bookmaker': bookmaker,
                        'label': label,
                        'line': outcome.get('line', '0.5'),
                        'odds': round(odds, 3) if odds else None,
                        'implied_prob': round(1 / odds, 3) if odds else None,
                        'true_win_prob': round(true_win_prob, 3) if true_win_prob else None,
                        'oddsshopper_ev': round(ev, 3) if ev else None
                    })
                    self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(OddsShopper))
