import asyncio
import json
import os
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.request_management import AsyncRequestManager


class VividPicksSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm

    async def start(self):
        url = 'https://api.betcha.one/v1/game/activePlayersForLeagueBoard'
        headers, json_data = {
            'Host': 'api.betcha.one',
            'x-px-authorization': '3:813cbf65244904a5212f61f40a60ed86a6f2f1a951def7deb5a99ddcf2f01b67:y1vgcvZN6Ft0n1AuKpbq7ky2Y9uxN6fMF22sGO7mbzy3FM5SW0tUsdOsM2OCTSZuIvuhdxVkcANEEmFBzkwPZw==:1000:lfXRuGZ3/0l/vJ0slGB0923Os2OyFyKuXFu9awQ/FN6W9iJkNUDGuMwzq+J2glanORDV6OA1InID5OG1mIdPnvXomHwAi07a05zRAlDRrfsZGlfodXmmbDFXp+Arm+DCi8mz+iQGJPldNrx9fEtSFSyWOFFdBLkgjzhQDj0raeFi/bfwdV9F9CIegRaRNtNlWftBmdYZWUiwK9OyrrTngFqeVm+LkbLn+olbEAZspnk=',
            'cache-control': 'no-cache',
            'x-px-device-model': 'iPhone 14 Pro',
            'x-px-device-fp': 'D9D02ACC-7330-4E54-BE30-81C3B80AA9CD',
            'betcha-version': 'ios(17.5.1)/230/83512be3',
            'betcha-timezone': 'America/Chicago',
            'user-agent': 'VividPicks/230 CFNetwork/1496.0.7 Darwin/23.5.0',
            'x-px-mobile-sdk-version': '3.0.3',
            'betcha-device': 'D9D02ACC-7330-4E54-BE30-81C3B80AA9CD',
            'baggage': 'sentry-environment=production,sentry-public_key=3da4c2e22ad24f418a283c246bb972a0,sentry-release=2.0.25%28230%29-83512be3,sentry-trace_id=2dc7d3ce69644329b9c80f91212dc785',
            'x-px-os-version': '17.5.1',
            'x-px-os': 'iOS',
            'authorization': 'eyJraWQiOiJFeUpLSkNtYW9wdUw1VHpXZVVKNmd0aENXSTA3SUNoalUzejJ6ZEJLaE9NPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJmMTkyZmEwYi1iYWY0LTRiYWMtYWM3Zi02MDdhMTEwMDkyYzQiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMi5hbWF6b25hd3MuY29tXC91cy1lYXN0LTJfSXpnOHRIVnFhIiwiY29nbml0bzp1c2VybmFtZSI6ImphY29ic29uc2hhd24zMyIsInByZWZlcnJlZF91c2VybmFtZSI6ImphY29ic29uc2hhd24zMyIsImN1c3RvbTp1c2VySWQiOiI2NmFhOWI5MDFkYTRlN2ZjYTcxMGFmYWMiLCJhdWQiOiI1cDlrcDE4dXFmcGtndjExYzFtYWk2dDZqZiIsImV2ZW50X2lkIjoiZTk3ODVjZGYtMjIwOC00N2RhLThjNWYtNWJiMzg4ODAxMjBlIiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE3MjI0NTY5NjUsIl9pZCI6IjY2YWE5YjkwMWRhNGU3ZmNhNzEwYWZhYyIsImV4cCI6MTcyNDEyMjYwMCwiaWF0IjoxNzI0MTE5MDAwLCJ1c2VybmFtZSI6ImphY29ic29uc2hhd24zMyJ9.h6FSDRpUYZwOOPeULGWJYggW-_wGjPTUd0wDffgogbIXzKyKJZM4E7iJbOM2Z2BlUzPZnF5ckEUF-riAsHDrKlgdcXB1r0sCUK_uLO4rOgpqX0zA-KJbPb4a8OSOvsWm0ZjbGdGxamOBEW-uZlLOoJEnHeXbSDzGu1lnrU2g2496FjU70tYCQHmYOLOJ-pYR0pSDOgFiWqVbAbCt_Srz4PHkSpVWEIdR9BoKfwcbgCC9_uCBtSxt5HBBltY2_VR_XF1a0dZ5OxkEToPD6gncB1F0XD1O3hkmLJAzYPdjTeh2bB7Ho7GuPQVKuNXh9_mqEhq_ZW1iYAZX2gVmYIo3YA',
            'accept-language': 'en-us',
            'x-px-vid': '6fcc897e-4f79-11ef-81f2-7a4e1f9004de',
            'accept': 'application/json',
            'content-type': 'application/json',
            'x-px-uuid': 'de460224-5e97-11ef-ac2f-ad7d00d797ac',
            'sentry-trace': '2dc7d3ce69644329b9c80f91212dc785-08ef37f394df43a6-0',
        }, {
            'league': 'Multi-Sport',
            'matchUp': False,
        }

        await self.arm.post(url, self._parse_lines, headers=headers, json=json_data)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()

        for event in data.get('gret', []):
            league, game_info, game_time = event.get('league'), event.get('gameInfo'), event.get('gameTime')
            for player in event.get('activePlayers', []):
                last_updated = player.get('updatedAt')
                subject, abv_team_name, position = player.get('name'), player.get('abvTeamName'), player.get('position')
                odds_jam_prop_sources = player.get('oddsJamProps')
                for prop in player.get('visiblePlayerProps', []):
                    multiplier_source, market, line, multiplier = '', prop.get('p'), prop.get('val'), 1.0
                    mult_player_props = player.get('configPlayerProps')
                    if mult_player_props:
                        mult_market = mult_player_props.get(market)
                        if mult_market:
                            multiplier = mult_market.get('multiplier', multiplier)
                            multiplier_source = mult_market.get('source')

                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'game_info': game_info,
                                'market_category': 'player_props',
                                'market': market,
                                'game_time': game_time,
                                'subject_team': abv_team_name,
                                'subject': subject,
                                'position': position,
                                'bookmaker': 'Vivid Picks',
                                'label': 'Over' if multiplier > 1.0 else 'Under',
                                'line': line,
                                'multiplier': multiplier,
                                'multiplier_source': multiplier_source,
                                'oddsjam_prop_sources': odds_jam_prop_sources
                            })

                        else:
                            for label in ['Over', 'Under']:
                                self.prop_lines.append({
                                    'batch_id': self.batch_id,
                                    'time_processed': datetime.now(),
                                    'last_updated': last_updated,
                                    'league': league,
                                    'game_info': game_info,
                                    'market_category': 'player_props',
                                    'market': market,
                                    'game_time': game_time,
                                    'abv_team_name': abv_team_name,
                                    'subject': subject,
                                    'position': position,
                                    'bookmaker': 'Vivid Picks',
                                    'label': label,
                                    'line': line,
                                    'multiplier': multiplier,
                                    'odds_jam_prop_sources': odds_jam_prop_sources
                                })

        relative_path = 'data_samples/vividpicks_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[Vivid Picks]: {len(self.prop_lines)} lines')


async def main():
    spider = VividPicksSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager())
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Vivid Picks]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
