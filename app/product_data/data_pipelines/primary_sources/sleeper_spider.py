import json
from collections import defaultdict
from datetime import datetime

import cloudscraper


class SleeperSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.scraper = cloudscraper.create_scraper()
        self.headers = {
            'Host': 'api.sleeper.app',
            'x-amp-session': '1724697278937',
            'accept': 'application/json',
            'authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdmF0YXIiOiIxNWQ3Y2YyNTliYzMwZWFiOGY2MTIwZjQ1ZjY1MmZiNiIsImRpc3BsYXlfbmFtZSI6IlNoYXdudGhlcmVhbHNoYWR5IiwiZXhwIjoxNzU2MjMzMzEyLCJpYXQiOjE3MjQ2OTczMTIsImlzX2JvdCI6ZmFsc2UsImlzX21hc3RlciI6ZmFsc2UsInJlYWxfbmFtZSI6bnVsbCwidXNlcl9pZCI6NzI5MjAyODc1NTk4Nzc0MjcyLCJ2YWxpZF8yZmEiOiJwaG9uZSJ9.hvc8FXdweWwNkBvrhCJ8ytRcBkX5ilDZa77IQtgleJM',
            'x-api-client': 'api.cached',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Sleeper/93.1.0 CFNetwork/1496.0.7 Darwin/23.5.0',
            'x-device-id': '71009696-F347-40AA-AE8C-5247A63041DF',
            'x-platform': 'ios',
            'x-build': '93.1.0',
            'x-bundle': 'com.blitzstudios.sleeperbot',
        }

    def start_requests(self):
        url = 'https://api.sleeper.app/players'
        params = {
            'exclude_injury': 'false',
        }

        response = self.scraper.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            self.parse_players(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_players(self, response):
        data = response.json()

        # get players
        players = dict()
        for player in data:
            team = player.get('team')
            if not team:
                continue

            first_name, last_name, position = player.get('first_name'), player.get('last_name'), player.get('position')
            sport, player_id, subject = player.get('sport'), player.get('player_id'), ' '.join([first_name, last_name])
            if player_id and sport:
                if sport not in players:
                    players[sport] = {}

                players[sport][player_id] = {'team': team, 'player_name': subject, 'position': position}

        url = 'https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&eg=15.control'

        response = self.scraper.get(url, headers=self.headers)
        if response.status_code == 200:
            self.parse_lines(response, players)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response, players):
        data = response.json()

        for line in data:
            team, subject, position, player_id, league = '', '', '', line.get('subject_id'), line.get('sport')
            if player_id:
                player = players.get(league).get(player_id)
                if player:
                    team, subject, position = player.get('team'), player.get('player_name'), player.get('position')

            last_updated, market = line.get('updated_at'), line.get('wager_type')
            if last_updated:
                # convert from unix to a datetime
                last_updated = datetime.fromtimestamp(last_updated / 1000)

            for option in line.get('options', []):
                label, line = option.get('outcome').title(), option.get('outcome_value')
                multiplier = option.get('payout_multiplier')

                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'last_updated': last_updated,
                    'league': league.upper(),
                    'market_category': 'player_props',
                    'market': market,
                    'subject': subject,
                    'bookmaker': 'Sleeper',
                    'label': label,
                    'line': line,
                    'multiplier': multiplier
                })

        with open('sleeper_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))


SleeperSpider(batch_id='123').start_requests()
