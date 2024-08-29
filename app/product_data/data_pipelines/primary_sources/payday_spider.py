import json
from datetime import datetime
import aiohttp
import asyncio


class PaydaySpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.session = None
        self.headers = {
            'Host': 'api.paydayfantasy.com',
            'accept': '*/*',
            'content-type': 'application/json',
            'user-agent': 'Betscoop/2 CFNetwork/1496.0.7 Darwin/23.5.0',
            'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5NDJmYWIwZi0zMTQzLTQ2NmQtOTQ3MS1jODBmYmM3ZGYzMGUiLCJqdGkiOiI1OTZiMjMwOTczMGJiYjEyNjliNWU2MGM5MmJjMWFmMmFmYTY5MWFmYmFiZWQ5ZWI0OTIwN2JkYjQ3Yjg5Nzg0M2U4ZjQyZTg1MzUyNmFhNCIsImlhdCI6MTcyNDA5NzY0MC44ODQ5MTgsIm5iZiI6MTcyNDA5NzY0MC44ODQ5MjcsImV4cCI6MTc1NTYzMzY0MC44NzU1MjUsInN1YiI6IjBiODI2Mzc3LTRkMTUtNDBlMC1iNjZiLTU4ZGI3YTQzOWU1MCIsInNjb3BlcyI6WyIqIl19.qvwpWkgMa25A1R35YdAwiH00hkKur8xyo70JJYALVWMb2ig-BUDRfXXWrOC-CW7t3Pgf1hx9uZQNrq0P5MtI6FgLyUBoEf-RprxvXNV3SPukk8W42M8DSJAmsmRMnc99nsDLJyGv85v1VdTXe_eF5B9lqCZIwyCiVhS3Sc-_lkTP7Hm0yAXFIMqrt6nmjhcloRwV-S0tdyr6V9DQhnxGPU-9NasolMNf41FeYLcxVaynID67XJFs3rqFBQUR6PLzQLAgV_3vmwVXLJWvIgdrASomYcOUvBuKu4eiAPnd6SL5KTVPfwtY8yk2TmafTZUw0YTDU6EBYNlloBqpV6UJ0Kw4MZTwyP0lOyfQznOQu8jSUGrXujtfHMOHDT9lpFNe4Xdl91aNz-JJ-K-tSZEQlcNxRNfDC3wuhYzW6Mg1oBj10pIl3rHF9HYjVccdxuHBE9hkgSk3VpqEfxV9px0sGY3M0-8kfoWuCHSk5VVI4hlpdganx1vFJgkuFRpGC5yQAFle8siVaAQ9k9I-C84tLewCSGDkNHyO1PU_yJ_ivBs6Zdj1ZasSw5pLT6PQmROWaq28Xx_iKxS_mTmQxSlVjEM4mxuWV-xfuvUX2tSf8GJw_91QoGpN-7hTckwXygnxaVwQ96lIu4LbXnwveWKbZZDINYv8d_uk5SAzzFX1pwU',
            'accept-language': 'en-US,en;q=0.9',
        }

    async def start_requests(self):
        url = 'https://api.paydayfantasy.com/api/v2/app/contests/total'
        params = {
            'include_solo_contests': '1',
        }

        async with aiohttp.ClientSession() as session:
            self.session = session
            async with self.session.get(url, headers=self.headers, params=params) as response:
                if response.status == 200:
                    await self.parse_leagues(response)
                else:
                    print(f"Failed to retrieve {url} with status code {response.status}")

    async def parse_leagues(self, response):
        data = await response.json()

        url = 'https://api.paydayfantasy.com/api/v2/app/contests'

        tasks = []
        for league_data in data.get('data', []):
            league = league_data.get('slug')
            if league:
                params = {
                    'leagues': league,
                    'include_solo_contests': '1',
                }

                tasks.append(self.fetch_and_parse_contests(url, params, league))

        await asyncio.gather(*tasks)

        with open('payday_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))

    async def fetch_and_parse_contests(self, url, params, league):
        async with self.session.get(url, headers=self.headers, params=params) as response:
            if response.status == 200:
                await self.parse_contests(response, league)
            else:
                print(f"Failed to retrieve {url} with status code {response.status}")

    async def parse_contests(self, response, league):
        data = await response.json()

        contests = data.get('data', {}).get('contests')
        if contests:
            parlay_contest = contests[0]
            if 'Parlay Contest' in parlay_contest.get('name'):
                parlay_contest_id = parlay_contest.get('id')

                url = f'https://api.paydayfantasy.com/api/v2/app/contests/{parlay_contest_id}/games'

                async with self.session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        await self.parse_lines(response, league)
                    else:
                        print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def parse_lines(self, response, league):
        data = await response.json()

        for game in data.get('data', {}).get('games', []):
            game_info = game.get('title')
            for player_prop in game.get('player_props', []):
                market, line, player = player_prop.get('name'), player_prop.get('value'), player_prop.get('player')
                if player:
                    subject, position = player.get('name'), player.get('position')

                    for label in ['Over', 'Under']:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market': market,
                            'subject': subject,
                            'bookmaker': 'Payday',
                            'label': label,
                            'line': line
                        })


async def main():
    spider = PaydaySpider(batch_id='12345')
    await spider.start_requests()

if __name__ == "__main__":
    asyncio.run(main())
