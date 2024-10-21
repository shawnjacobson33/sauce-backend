import asyncio
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league


class MoneyLine(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url()
        headers = self.packager.get_headers()
        cookies = self.packager.get_cookies()
        params = self.packager.get_params()
        await self.rm.get(url, self._parse_lines, headers=headers, cookies=cookies, params=params)

    async def _parse_lines(self, response):
        data = response.json()
        subject_ids = dict()
        for bet in data.get('bets', []):
            is_boosted, league, market = False, bet.get('league'), bet.get('bet_text')
            if league:
                league = clean_league(league)
                if not Packager.is_league_good(league):
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

            market = clean_market(market)
            market_id = self.ds.get_market_id(Market(market, league))
            subject_id, subject, subject_team, subject_components = None, None, None, bet.get('title')
            if subject_components:
                subject_components = subject_components.split()
                team_components = subject_components[-1]
                subject, subject_team = ' '.join(subject_components[:-1]), team_components[1:-1].replace('r.(', '')
                if subject:
                    subject = clean_subject(subject)
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team))
                        subject_ids[f'{subject}{subject_team}'] = subject_id

            for i in range(1, 3):
                label, line, option_components = None, None, bet.get(f'option_{i}')
                if option_components:
                    option_components = option_components.split()
                    if len(option_components) == 2:
                        label, line = option_components[0].lower().title(), option_components[1]

                # update shared data
                PropLines.update(''.join(self.info.name.split()).lower(), {
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': self.info.name,
                    'label': label,
                    'line': line,
                    'odds': self.info.default_payout.odds,
                    'is_boosted': is_boosted
                })
                self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(MoneyLine))
