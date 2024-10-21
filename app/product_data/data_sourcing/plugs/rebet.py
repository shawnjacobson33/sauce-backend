from datetime import datetime
import asyncio

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager
from app.product_data.data_sourcing.plugs.helpers.helpers import run, is_league_good
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league


class Rebet(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url(name='tourney_ids')
        headers = self.packager.get_headers(name='tourney_ids')
        json_data = self.packager.get_json_data(name='tourney_ids')
        await self.rm.post(url, self._parse_tourney_ids, headers=headers, json=json_data)

    async def _parse_tourney_ids(self, response):
        data = response.json()
        # get tournament_ids
        tournament_ids = dict()
        for league in data.get('data', []):
            league_name, tournament_id = league.get('title'), league.get('tournament_id')
            if league_name and tournament_id:
                tournament_ids[tournament_id] = league_name

        url = self.packager.get_url()
        headers = self.packager.get_headers()
        tasks = []
        for tournament_id in tournament_ids:
            json_data = self.packager.get_json_data(var=tournament_id)
            tasks.append(self.rm.post(url, self._parse_lines, headers=headers, json=json_data))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response):
        data = response.json()
        subject_ids = dict()
        for event in data.get('data', {}).get('events', []):
            league, game_time = event.get('league_name'), event.get('start_time')
            if league:
                league = clean_league(league)
                if not is_league_good(league):
                    continue

            odds = event.get('odds')
            if odds:
                for market in odds.get('market', []):
                    tab_name = market.get('tab_name')
                    if tab_name == 'Player Props':
                        # get market
                        market_name, market_id = market.get('name'), None
                        if '{%player}' in market_name:
                            market_name_components = market_name.split(' (')
                            new_market_name_components = market_name_components[0].split()
                            market_name = ' '.join(new_market_name_components[1:]).title()

                        if market_name:
                            market_name = clean_market(market_name)
                            market_id = self.ds.get_market_id(Market(market_name, league))

                        # get subject
                        subject_id, subject, player_name = None, None, market.get('player_name')
                        if player_name:
                            player_name_components = player_name.split(', ')
                            if len(player_name_components) == 2:
                                subject = clean_subject(f'{player_name_components[1]} {player_name_components[0]}')
                                subject_id = subject_ids.get(f'{subject}{league}')
                                if not subject_id:
                                    subject_id = self.ds.get_subject_id(Subject(subject, league))
                                    subject_ids[f'{subject}{league}'] = subject_id

                        # get line
                        label, line, outcomes = None, None, market.get('outcome', [])
                        if isinstance(outcomes, list):
                            for outcome in outcomes:
                                outcome_name, the_odds = outcome.get('name'), outcome.get('odds')
                                if (the_odds == '1.001') or (not the_odds):
                                    continue

                                if outcome_name:
                                    outcome_name_components = outcome_name.split()
                                    if ('over' in outcome_name) or ('under' in outcome_name):
                                        label, line = outcome_name_components[0].title(), outcome_name_components[1]
                                    elif '+' in outcome_name:
                                        label, line = 'Over', outcome_name_components[-1][:-1]

                                PropLines.update(''.join(self.info.name.split()).lower(), {
                                    'batch_id': self.batch_id,
                                    'time_processed': datetime.now(),
                                    'league': league,
                                    'market_category': 'player_props',
                                    'market_id': market_id,
                                    'market': market_name,
                                    'subject_id': subject_id,
                                    'subject': subject,
                                    'bookmaker': self.info.name,
                                    'label': label,
                                    'line': line,
                                    'odds': the_odds,
                                })
                                self.data_size += 1
                        else:
                            outcome_name, the_odds = outcomes.get('name'), outcomes.get('odds')
                            if (the_odds == '1.001') or (not the_odds):
                                continue

                            if outcome_name:
                                outcome_name_components = outcome_name.split()
                                if ('over' in outcome_name) or ('under' in outcome_name):
                                    label, line = outcome_name_components[0].title(), outcome_name_components[1]
                                elif '+' in outcome_name:
                                    label, line = 'Over', outcome_name_components[-1][:-1]

                            # update shared data
                            PropLines.update(''.join(self.info.name.split()).lower(), {
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'league': league,
                                'market_category': 'player_props',
                                'market_id': market_id,
                                'market': market_name,
                                'subject_id': subject_id,
                                'subject': subject,
                                'bookmaker': self.info.name,
                                'label': label,
                                'line': line,
                                'odds': the_odds,
                            })
                            self.data_size += 1


if __name__ == "__main__":
    asyncio.run(run(Rebet))
