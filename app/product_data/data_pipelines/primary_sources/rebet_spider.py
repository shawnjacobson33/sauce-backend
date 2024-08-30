import json
import os
import time
import uuid
from datetime import datetime

import asyncio

from app.product_data.data_pipelines.request_management import AsyncRequestManager


class RebetSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm

    async def start(self):
        url = 'https://api.rebet.app/prod/sportsbook-v3/get-new-odds-leagues'
        headers, json_data = {
            'Host': 'api.rebet.app',
            'content-type': 'application/json',
            'accept': 'application/json, text/plain, */*',
            'baggage': 'sentry-environment=production,sentry-public_key=d4c222a7c513d9292ee6814277c6e1aa,sentry-release=com.rebet.app%408.97%2B399,sentry-trace_id=b6143e04f2ef4c4fa01c923aceca8cd8',
            'accept-language': 'en-US,en;q=0.9',
            'x-api-key': 'J9xowBQZM980G97zv9VoB9Ylady1pVtS5Ix9tuL1',
            'sentry-trace': 'b6143e04f2ef4c4fa01c923aceca8cd8-69435e3a704f4230-0',
            'user-agent': 'rebetMobileApp/399 CFNetwork/1496.0.7 Darwin/23.5.0',
            'x_api_key': 'J9xowBQZM980G97zv9VoB9Ylady1pVtS5Ix9tuL1',
        }, {
            'league_name': [],
        }

        await self.arm.post(url, self._parse_tourney_ids, headers=headers, json=json_data)

    async def _parse_tourney_ids(self, response):
        data = response.json()

        # get tournament_ids
        tournament_ids = dict()
        for league in data.get('data', []):
            league_name, tournament_id = league.get('title'), league.get('tournament_id')
            if league_name and tournament_id:
                tournament_ids[tournament_id] = league_name

        url = 'https://api.rebet.app/prod/sportsbook-v3/load-sportsbook-data-v3'
        headers = {
            'Host': 'api.rebet.app',
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'authorization': 'Bearer eyJraWQiOiI3WkdkV1Y5THJucmdIY25QUWdMNWd0VzJXSGlpV2o3K2VBQ1FsR2FQeGlVPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhOWE4NmE3My0yMTYwLTQyM2UtOTViOS02NTA0ZGI3ZTJjODciLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiYWRkcmVzcyI6eyJmb3JtYXR0ZWQiOiJNaW5uZWFwb2xpcywgTWlubmVzb3RhIn0sImJpcnRoZGF0ZSI6IjIwMDUtMDctMjciLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0yLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMl9lcHo2ZEF4UXUiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOmZhbHNlLCJjb2duaXRvOnVzZXJuYW1lIjoidGhlcmVhbHNsaW0iLCJvcmlnaW5fanRpIjoiNDk2ZWJjMWItYTYyYS00ZjkxLWEwOTctNjRlODgyOGQ2ZTcxIiwiYXVkIjoiNzN2dnZsY2tkcmpqb3ZiaXNzM2loYmQyNm8iLCJldmVudF9pZCI6ImUyMWYyMDBkLTk5ZjMtNDM4Mi05YTk3LTUyOGNhYzgwOTY4ZiIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNzI0NDI2ODAyLCJwaG9uZV9udW1iZXIiOiIrMTUwNzg4NDAyODYiLCJleHAiOjE3MjQ1MTMyMDIsImlhdCI6MTcyNDQyNjgwMiwianRpIjoiYzY3MWIwM2YtZWMzZC00ZWY0LWJkNjktNDkzZGUyZmFiNzJlIiwiZW1haWwiOiJzaGF3bmphY29ic29uMzNAZ21haWwuY29tIn0.F8YZwRmOADpUNuWmJ6wjFk-fZvGMopa0KM8kcf_vbvWv_Kik8czycPpERpNS1T8gtjcUUni12Qt9JjF15vSryaQScYw6pCft9q0yFrXjMNReIHgTNVNQQiFA6vrto2VdF4t4D7iyxyfU4dRdr95b2tpzm9VjsHNIKahqj76OrdWzjUQMLtBCqw-e7Wm9jm3TaMMTHlcjIefw0JUBiJlL9Lk_mqKtwKObX9tRTaqIS9XvXRvh7Hz_1W6btwZiEzx37ponn7L-AqnSArXTKDl_rzgN0qr8U4KBTCHdcJx5j5x8t2DeTrm-NKh3oF5AX4qasbiy77YFpLyBP2pVkkqVgw',
            'sentry-trace': 'b6143e04f2ef4c4fa01c923aceca8cd8-69435e3a704f4230-0',
            'baggage': 'sentry-environment=production,sentry-public_key=d4c222a7c513d9292ee6814277c6e1aa,sentry-release=com.rebet.app%408.97%2B399,sentry-trace_id=b6143e04f2ef4c4fa01c923aceca8cd8',
            'user-agent': 'rebetMobileApp/399 CFNetwork/1496.0.7 Darwin/23.5.0',
            'accept-language': 'en-US,en;q=0.9',
        }

        tasks = []
        for tournament_id in tournament_ids:
            json_data = {
                'tournament_id': tournament_id,
                'game_type': 3,
            }

            tasks.append(self.arm.post(url, self._parse_lines, headers=headers, json=json_data))

        await asyncio.gather(*tasks)

        relative_path = 'data_samples/rebet_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[Rebet]: {len(self.prop_lines)} lines')

    async def _parse_lines(self, response):
        data = response.json()

        for event in data.get('data', {}).get('events', []):
            last_updated, league, game_time = event.get('updated_at'), event.get('league_name'), event.get('start_time')
            if last_updated:
                # convert from unix to a datetime
                last_updated = datetime.fromtimestamp(int(last_updated) / 1000)

            odds = event.get('odds')
            if odds:
                for market in odds.get('market', []):
                    tab_name = market.get('tab_name')
                    if tab_name == 'Player Props':
                        # get market
                        market_name = market.get('name')
                        if '{%player}' in market_name:
                            market_name_components = market_name.split(' (')
                            new_market_name_components = market_name_components[0].split()
                            market_name = ' '.join(new_market_name_components[1:]).title()

                        # get subject
                        subject, player_name = '', market.get('player_name')
                        if player_name:
                            player_name_components = player_name.split(', ')
                            if len(player_name_components) == 2:
                                subject = f'{player_name_components[1]} {player_name_components[0]}'

                        # get line
                        label, line, outcomes = '', '', market.get('outcome', [])
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

                                rebet_probability = outcome.get('probabilities')

                                self.prop_lines.append({
                                    'batch_id': self.batch_id,
                                    'time_processed': datetime.now(),
                                    'last_updated': last_updated,
                                    'league': league,
                                    'market_category': 'player_props',
                                    'market': market_name,
                                    'game_time': game_time,
                                    'subject': subject,
                                    'bookmaker': 'Rebet',
                                    'label': label,
                                    'line': line,
                                    'odds': the_odds,
                                    'rebet_probability': rebet_probability
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

                            rebet_probability = outcomes.get('probabilities')

                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'market_category': 'player_props',
                                'market': market_name,
                                'game_time': game_time,
                                'subject': subject,
                                'bookmaker': 'Rebet',
                                'label': label,
                                'line': line,
                                'odds': the_odds,
                                'rebet_probability': rebet_probability
                            })


async def main():
    spider = RebetSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager())
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Rebet]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
