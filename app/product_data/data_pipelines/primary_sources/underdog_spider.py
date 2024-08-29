import json
from datetime import datetime

import cloudscraper


class UnderdogSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id

    def start_requests(self):
        url = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjNnRTM4R1FUTW1lcVA5djFYVllEUCJ9.eyJ1ZF9zdWIiOiJhNWFlYzNhNy05YWZhLTQ5NjQtODhmMC0yMDg3YjZlMmI3MjAiLCJ1ZF9lbWFpbCI6ImphY29ic29uc2hhd24zM0BnbWFpbC5jb20iLCJ1ZF91c2VybmFtZSI6InNsaW1zaGFkeTMzMyIsImlzcyI6Imh0dHBzOi8vbG9naW4udW5kZXJkb2dzcG9ydHMuY29tLyIsInN1YiI6ImF1dGgwfGE1YWVjM2E3LTlhZmEtNDk2NC04OGYwLTIwODdiNmUyYjcyMCIsImF1ZCI6Imh0dHBzOi8vYXBpLnVuZGVyZG9nZmFudGFzeS5jb20iLCJpYXQiOjE3MjM1ODI1NjgsImV4cCI6MTcyMzU4NjE2OCwic2NvcGUiOiJvZmZsaW5lX2FjY2VzcyIsImd0eSI6WyJyZWZyZXNoX3Rva2VuIiwicGFzc3dvcmQiXSwiYXpwIjoiY1F2WXoxVDJCQUZiaXg0ZFlSMzdkeUQ5TzBUaGYxczYifQ.Bq5SXeHZ_tujrI-VQR3m7rTdyAquLwUYaxPoqZdo5BMRuHB4tQU4_i77A69PGRBaeiJ6n6qCRc_PZkRyl-atdcDzr5ewYsdwCzexNCNUKcOuSnLK1rF2ELz-sUkm8hWrCclUqSDWGnnl9ywDpbnVqMQqa3fQKZG9OOPObK77IFR0khoSK7MN2IrKbhi7JDnRvD8Sc7_iEfusjvpAc0K5lRVXIfWdXKeG0m7g6InKJQLR55vNUicSbsA40dQvdQyZuP_oRE5TcPe95z4ji9-7ut-Gl9_Er9oKs-2tIRyVCLTbjl5Ig_JIH6CszaUh887fRpfJSAdaO8eHfPsxxLyvHw',
            'client-device-id': '3ec5c852-be11-4848-a59c-a728187c7def',
            'client-request-id': '4264d803-5c8e-4d1b-90ce-794d695324fa',
            'client-type': 'web',
            'client-version': '20240813192008',
            'origin': 'https://underdogfantasy.com',
            'priority': 'u=1, i',
            'referer': 'https://underdogfantasy.com/',
            'referring-link': '',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'user-latitude': '20.6968263',
            'user-location-token': '',
            'user-longitude': '-156.441985',
        }

        response = cloudscraper.create_scraper().get(url, headers=headers)
        if response.status_code == 200:
            self.parse_lines(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response):
        # get body content in json format
        data = response.json()

        # first pass need to collect game/match info ids for each appearance
        game_ids, player_ids = dict(), dict()
        for appearance in data.get('appearances', []):
            appearance_id, appearance_match_id = appearance.get('id'), appearance.get('match_id')
            appearance_player_id = appearance.get('player_id')
            if appearance_id:
                if appearance_match_id:
                    game_ids[appearance_id] = appearance_match_id
                if appearance_player_id:
                    player_ids[appearance_id] = appearance_player_id

        # second pass to collect actual match data
        game_data = dict()
        for game in data.get('games', []):
            game_id = game.get('id')
            if game_id:
                game_data[game_id] = game

        # third pass to collect actual solo game data
        solo_game_data = dict()
        for game in data['solo_games']:
            game_id = game.get('id')
            if game_id:
                solo_game_data[game_id] = game

        # fourth pass to collect player data
        player_data = dict()
        for player in data.get('players', []):
            player_id = player.get('id')
            if player_id:
                player_data[player_id] = player

        for game in data.get('over_under_lines', []):
            line = game.get('stat_value')
            for option in game.get('options', []):
                # retrieve the match data corresponding to the appearance id of this line
                league, market, subject, game_time, over_under = '', '', '', '', game.get('over_under')
                if over_under:
                    appearance_stat = over_under.get('appearance_stat')
                    if appearance_stat:
                        appearance_id = appearance_stat.get('appearance_id')
                        market = appearance_stat.get('display_stat')
                        if appearance_id:
                            game_id = game_ids.get(appearance_id)
                            if game_id:
                                match = game_data.get(game_id)
                                if not match:
                                    match = solo_game_data.get(game_id)

                                league, game_time = match.get('sport_id'), match.get('scheduled_at')

                            player_id = player_ids.get(appearance_id)
                            if player_id:
                                player = player_data.get(player_id)
                                # handle subject names
                                first_name, last_name = player.get('first_name'), player.get('last_name')
                                if not first_name:
                                    subject = last_name
                                elif not last_name:
                                    subject = first_name
                                else:
                                    subject = ' '.join([first_name, last_name])

                label = 'Over' if option.get('choice') == 'higher' else 'Under'
                multiplier = option.get('payout_multiplier')

                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'game_time': game_time,
                    'market_category': 'player_props',
                    'market': market,
                    'subject': subject,
                    'bookmaker': 'Underdog Fantasy',
                    'label': label,
                    'line': line,
                    'multiplier': multiplier
                })

        with open('../data_samples/underdog_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))


UnderdogSpider(batch_id="asd-123").start_requests()
