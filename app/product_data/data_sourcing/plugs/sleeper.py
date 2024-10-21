import asyncio
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.constants import FANTASY_SCORE_MAP
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class Sleeper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.headers = self.packager.get_headers()

    async def start(self):
        url = self.packager.get_url(name='players')
        params = self.packager.get_params()
        await self.rm.get(url, self._parse_players, headers=self.headers, params=params)

    async def _parse_players(self, response):
        data = response.json()
        # get players
        players = dict()
        for player in data:
            subject_team = player.get('team')
            if not subject_team:
                continue

            first_name, last_name, position = player.get('first_name'), player.get('last_name'), player.get('position')
            sport, player_id, subject = player.get('sport'), player.get('player_id'), ' '.join([first_name, last_name])
            if player_id and sport:
                if sport not in players:
                    players[sport] = {}

                players[sport][player_id] = {'subject_team': subject_team, 'player_name': subject, 'position': position}

        url = self.packager.get_url()
        await self.rm.get(url, self._parse_lines, players, headers=self.headers)

    async def _parse_lines(self, response, players):
        data = response.json()
        subject_ids = dict()
        for line in data:
            subject_id, subject_team, subject, position, cleaned_league = None, None, None, None, None
            player_id, league = line.get('subject_id'), line.get('sport')
            if league:
                cleaned_league = clean_league(league)
                if not Packager.is_league_good(cleaned_league):
                    continue

            if player_id:
                player = players.get(league).get(player_id)
                if player:
                    subject_team, subject, position = player.get('subject_team'), player.get('player_name'), player.get(
                        'position')
                    if position:
                        position = clean_position(position)

                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, cleaned_league, subject_team, position))
                            subject_ids[f'{subject}{subject_team}'] = subject_id

            market_id, last_updated, market = None, line.get('updated_at'), line.get('wager_type')
            if market:
                if market == 'fantasy_points':
                    market = FANTASY_SCORE_MAP.get(league, market)

                market = clean_market(market)
                market_id = self.ds.get_market_id(Market(market, cleaned_league))

            for option in line.get('options', []):
                label, line = option.get('outcome').title(), option.get('outcome_value')
                odds = option.get('payout_multiplier')
                PropLines.update(''.join(self.info.name.split()).lower(), {
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': cleaned_league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': self.info.name,
                    'label': label,
                    'line': line,
                    'odds': odds
                })
                self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(Sleeper))
