import asyncio
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_position


class Drafters(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url()
        headers = self.packager.get_headers()
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
                    if position == 'G':
                        continue

                    position = clean_position(position.strip())

                market_id, market, line = None, player.get('bid_stats_name'), player.get('bid_stats_value')
                if market:
                    market = clean_market(market)
                    if market in {'Significant Strikes', 'Takedowns'}:
                        continue

                    market_id = self.ds.get_market_id(Market(market))

                if subject:
                    subject = clean_subject(subject)
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        subject_id = self.ds.get_subject_id(Subject(subject, team=subject_team, position=position), user='Drafters')
                        subject_ids[f'{subject}{subject_team}'] = subject_id

                for label in ['Over', 'Under']:
                    # update shared data
                    PropLines.update(''.join(self.info.name.split()).lower(), {
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'game_info': game_info,
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
    asyncio.run(helper.run(Drafters))
