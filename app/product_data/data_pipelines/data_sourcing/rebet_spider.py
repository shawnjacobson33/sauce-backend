import json
import os
import time
import uuid
from datetime import datetime

import asyncio
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper


class RebetSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Rebet')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url(name='tourney_ids')
        headers = self.helper.get_headers(name='tourney_ids')
        json_data = self.helper.get_json_data(name='tourney_ids')
        await self.rm.post(url, self._parse_tourney_ids, headers=headers, json=json_data)

    async def _parse_tourney_ids(self, response):
        data = response.json()
        # get tournament_ids
        tournament_ids = dict()
        for league in data.get('data', []):
            league_name, tournament_id = league.get('title'), league.get('tournament_id')
            if league_name and tournament_id:
                tournament_ids[tournament_id] = league_name

        url = self.helper.get_url()
        headers = self.helper.get_headers()
        tasks = []
        for tournament_id in tournament_ids:
            json_data = self.helper.get_json_data(var=tournament_id)
            tasks.append(self.rm.post(url, self._parse_lines, headers=headers, json=json_data))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_lines(self, response):
        data = response.json()
        for event in data.get('data', {}).get('events', []):
            last_updated, league, game_time = event.get('updated_at'), event.get('league_name'), event.get('start_time')
            if league:
                league = DataCleaner.clean_league(league)

            if last_updated:
                # convert from unix to a datetime
                last_updated = datetime.fromtimestamp(int(last_updated) / 1000)

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
                            market_id = self.dn.get_market_id(market)

                        # get subject
                        subject_id, subject, player_name = None, None, market.get('player_name')
                        if player_name:
                            player_name_components = player_name.split(', ')
                            if len(player_name_components) == 2:
                                subject = f'{player_name_components[1]} {player_name_components[0]}'
                                if subject:
                                    subject_id = self.dn.get_subject_id(subject, league)

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

                                self.prop_lines.append({
                                    'batch_id': self.batch_id,
                                    'time_processed': datetime.now(),
                                    'last_updated': last_updated,
                                    'league': league,
                                    'market_category': 'player_props',
                                    'market_id': market_id,
                                    'market': market_name,
                                    'game_time': game_time,
                                    'subject_id': subject_id,
                                    'subject': subject,
                                    'bookmaker': 'Rebet',
                                    'label': label,
                                    'line': line,
                                    'odds': the_odds,
                                })
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

                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'market_category': 'player_props',
                                'market_id': market_id,
                                'market': market_name,
                                'game_time': game_time,
                                'subject_id': subject_id,
                                'subject': subject,
                                'bookmaker': 'Rebet',
                                'label': label,
                                'line': line,
                                'odds': the_odds,
                            })


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = RebetSpider(uuid.uuid4(), RequestManager(), DataNormalizer('Rebet', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Rebet]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
