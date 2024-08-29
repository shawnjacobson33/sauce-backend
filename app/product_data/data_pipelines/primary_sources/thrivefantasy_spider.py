import json
from datetime import datetime

import cloudscraper


class ThriveFantasySpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id

    def start_requests(self):
        url = 'https://api.thrivefantasy.com/houseProp/upcomingHouseProps'
        headers = {
            'Host': 'api.thrivefantasy.com',
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'x-tf-version': '1.1.39',
            'user-agent': 'ThriveFantasy/1 CFNetwork/1496.0.7 Darwin/23.5.0',
            'token': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0aGVyZWFsc2xpbSIsImF1ZGllbmNlIjoiSU9TIiwicGFzc3dvcmRMYXN0Q2hhbmdlZEF0IjpudWxsLCJpYXQiOjE3MjQwOTgwNDIsImV4cCI6MTcyNTgyNjA0Mn0.5NJJGuSmqGfPYYWwOWzr7t6BcJVvyZLgokvGgfbbCVOZz-rLjQYuWv2g_iQ-0A6mxTklflCH7Gv2XoMfrB8u6Q',
        }
        json_data = {
            'half': 0,
            'currentPage': 1,
            'currentSize': 20,
        }

        response = cloudscraper.create_scraper().post(url=url, headers=headers, json=json_data)
        if response.status_code == 200:
            self.parse_lines(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response):
        # get body content in json format
        data = response.json().get('response')

        for prop in data.get('data', []):
            contest_prop = prop.get('contestProp')

            # game info
            game_info = ''
            admin_event = contest_prop.get('adminEvent')
            if admin_event:
                home_team, away_team = admin_event.get('homeTeam'), admin_event.get('awayTeam')
                if home_team and away_team:
                    game_info = ' @ '.join([away_team, home_team])

            # player info
            player = contest_prop.get('player1')
            league, team, position, subject, market = '', '', '', '', ''
            if player:
                league = player.get('leagueType')
                team = player.get('teamAbbr')
                position = player.get('positionAbbreviation')

                # player name
                first_name, last_name = player.get('firstName'), player.get('lastName')
                if first_name and last_name:
                    subject = ' '.join([first_name, last_name])

                # market
                prop_params = player.get('propParameters')
                if prop_params and len(prop_params) > 1:
                    market = ' + '.join(prop_params)
                else:
                    market = prop_params[0]

            line = contest_prop.get('propValue')

            for label in ['Over', 'Under']:
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market': market,
                    'game_info': game_info,
                    'team': team,
                    'subject': subject,
                    'position': position,
                    'bookmaker': 'Thrive Fantasy',
                    'label': label,
                    'line': line
                })

        with open('thrivefantasy_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))


ThriveFantasySpider(batch_id='123').start_requests()
