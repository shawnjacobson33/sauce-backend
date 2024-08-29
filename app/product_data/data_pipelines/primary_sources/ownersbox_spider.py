import json
from datetime import datetime
import aiohttp
import asyncio


class OwnersBoxSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.session = None
        self.headers = {
            'Host': 'app.ownersbox.com',
            'accept': 'application/json',
            'content-type': 'application/json',
            'user-agent': 'OwnersBox/145 CFNetwork/1496.0.7 Darwin/23.5.0',
            'ownersbox_version': '7.12.0',
            'accept-language': 'en-US,en;q=0.9',
            'ownersbox_device': 'ios',
        }
        self.cookies = {
            'obauth': 'eyJhbGciOiJIUzUxMiJ9.eyJvYnRva2VuIjoiMFE2SzJRMVkxTjhEIiwidXNlclN0YXRlIjoiQUNUSVZFIiwiaXNzIjoiT3duZXJzQm94IiwidmVyaWZpZWQiOmZhbHNlLCJ0b2tlbkV4cGlyeSI6MTcyNDEyMDAxMjc4MCwic2Vzc2lvbklkIjozNDE5MDU1MDk2fQ.9dcOi9DJ8_R1PTD4-m3VqXebAj1pZ0LAFzaXsaIGkIsvrjLOjF9jW5KNQEoDYOKjLhyzahtGd7VObdR1ABwNUA',
        }

    async def start_requests(self):
        url = 'https://app.ownersbox.com/fsp-marketing/getSportInfo'

        async with aiohttp.ClientSession() as session:
            self.session = session
            async with session.get(url, headers=self.headers, cookies=self.cookies) as response:
                if response.status == 200:
                    await self.parse_leagues(response)
                else:
                    print(f"Failed to retrieve {url} with status code {response.status}")
                    print(response.content)

    async def parse_leagues(self, response):
        data = await response.json()

        tasks = []
        for league in data:
            url = 'https://app.ownersbox.com/fsp/v2/market'
            params = {
                'sport': league,
            }

            tasks.append(self.fetch_and_parse_lines(url, params))

        await asyncio.gather(*tasks)

        with open('ownersbox_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))

    async def fetch_and_parse_lines(self, url, params):
        async with self.session.get(url, headers=self.headers, cookies=self.cookies, params=params) as response:
            if response.status == 200:
                await self.parse_lines(response)
            else:
                print(f"Failed to retrieve {url} with status code {response.status}")

    async def parse_lines(self, response):
        # get body content in json format
        data = await response.json()

        for prop_line in data.get('markets', []):
            league = prop_line.get('sport')

            # get market
            market, market_type = '', prop_line.get('marketType')
            if market_type:
                market = market_type.get('name')

            # get game info
            game_info, game = '', prop_line.get('game')
            if game:
                away_team, home_team = game.get('awayTeam'), game.get('homeTeam')
                if away_team and home_team:
                    away_team_alias, home_team_alias = away_team.get('alias').upper(), home_team.get('alias').upper()
                    game_info = ' @ '.join([away_team_alias, home_team_alias])

            # get player
            team, position, subject, player = '', '', '', prop_line.get('player')
            if player:
                team, position = player.get('teamAlias').upper(), player.get('position')
                first_name, last_name = player.get('firstName'), player.get('lastName')
                subject = ' '.join([first_name, last_name])

            # get line
            line, balanced_line = 0, prop_line.get('line')
            if balanced_line:
                line = balanced_line.get('balancedLine')

            pick_options = prop_line.get('pickOptions')
            if pick_options:
                if ('MORE' in pick_options) and ('LESS' in pick_options):
                    for label in ['Over', 'Under']:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market': market,
                            'team': team,
                            'position': position,
                            'subject': subject,
                            'bookmaker': 'OwnersBox',
                            'label': label,
                            'line': line
                        })
                else:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'league': league,
                        'game_info': game_info,
                        'market_category': 'player_props',
                        'market': market,
                        'team': team,
                        'position': position,
                        'subject': subject,
                        'bookmaker': 'OwnersBox',
                        'label': 'Over' if 'MORE' in pick_options else 'Under',
                        'line': line
                    })


async def main():
    spider = OwnersBoxSpider(batch_id='12345')
    await spider.start_requests()

if __name__ == "__main__":
    asyncio.run(main())
