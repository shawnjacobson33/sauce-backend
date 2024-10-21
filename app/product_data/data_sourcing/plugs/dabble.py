from datetime import datetime
import asyncio

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class Dabble(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
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

    async def _parse_events(self, response, league):
        data = response.json()
        tasks = []
        for event in data.get('data', []):
            event_id, game_info = event.get('id'), event.get('name')

            if event.get('isDisplayed'):
                url = self.packager.get_url().format(event_id)
                tasks.append(self.rm.get(url, self._parse_lines, league, game_info))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league, game_info):
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
            if position:
                position = clean_position(position)

            subject_id, subject, subject_team = None, player_prop.get('playerName'), player_prop.get('teamAbbreviation')
            if subject_team:
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
            # update shared data
            PropLines.update(''.join(self.info.name.split()).lower(), {
                'batch_id': self.batch_id,
                'time_processed': datetime.now(),
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
            self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(Dabble))
