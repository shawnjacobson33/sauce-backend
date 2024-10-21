import asyncio
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager
from app.product_data.data_sourcing.utils.constants import LEAGUE_SPORT_MAP, IN_SEASON_LEAGUES
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


STATISTICS = {
    'Football': [
        'Passing%2520Yards',
        'Pass%2520Completions',
        'Passing%2520TDs',
        'Pass%2520Attempts',
        'Rushing%2520Yards',
        'Carries',
        'Receiving%2520Yards',
        'Receptions',
        'Pass%2520Interceptions',
        'Tackles',
        'Sacks',
        'Interceptions'
    ],
    'Basketball': [
        'Points',
        'Rebounds',
        'Assists',
        'Pts%2520%252B%2520Reb%2520%252B%2520Ast',
        'Three%2520Point%2520Field%2520Goals%2520Made'
    ],
    'Baseball': [
        'Strikeouts',
        'Hits%2520Allowed',
        'Earned%2520Runs',
        'Hits',
        'Runs%2520%252B%2520RBIs',
        'Total%2520Bases',
        'Stolen%2520Bases'
    ],
    'Ice Hockey': [
        'Points',
        'Shots%2520On%2520Goal'
    ]
}
LEAGUE_MAP = {
    'NCAAF': 'NCAAFB'
}
LEAGUES = [LEAGUE_MAP.get(league, league).lower() for league in IN_SEASON_LEAGUES]


class BetOnline(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        tasks = []
        for league in LEAGUES:
            url = self.packager.get_url(name='games')
            headers = self.packager.get_headers(name='games')
            params = self.packager.get_params(name='games', var_1=league)
            tasks.append(self.rm.get(url, self._parse_games, league, headers=headers, params=params))

        await asyncio.gather(*tasks)

    async def _parse_games(self, response, league):
        data = response.json()
        tasks = []
        for game in data:
            game_id, providers = None, game.get('providers')
            if providers:
                for provider in providers:
                    if provider.get('name') == 'nix':
                        game_id = provider.get('id')

            if league and game_id:
                league = clean_league(league)
                url = self.packager.get_url()
                headers = self.packager.get_headers()
                league_statistics = STATISTICS.get(LEAGUE_SPORT_MAP.get(league))
                for statistic in league_statistics:
                    params = self.packager.get_params(var_1=game_id, var_2=statistic)
                    tasks.append(self.rm.get(url, self._parse_lines, league, headers=headers, params=params))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league):
        data = response.json()
        subject_ids = dict()
        for prop_line in data:
            market_id, market = None, prop_line.get('statistic')
            if market:
                market = clean_market(market)
                market_id = self.ds.get_market_id(Market(market, league))

            for player in prop_line.get('players', []):
                position, position_data = None, player.get('position')
                if position_data:
                    position = position_data.get('title')
                    if position:
                        position = clean_position(position)

                subject_team = player.get('team')
                subject_id, subject = None, player.get('name')
                if subject:
                    subject = clean_subject(subject)
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                        subject_ids[f'{subject}{subject_team}'] = subject_id

                for market_data in player.get('markets', []):
                    if market_data.get('isActive') and market_data.get('isActual'):
                        line = market_data.get('value')
                        decimal_odds = market_data.get('odds')
                        label, condition = None, market_data.get('condition')
                        if condition:
                            label = 'Over' if condition == 1 else 'Under'

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
                            'odds': decimal_odds,
                            'implied_prob': round(1 / float(decimal_odds), 3)
                        })
                        self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(BetOnline))

