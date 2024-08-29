import json
from datetime import datetime

import cloudscraper


class ParlayPlaySpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id

    def start_requests(self):
        url = 'https://parlayplay.io/api/v1/crossgame/search/?sport=All&league=&includeAlt=true'
        headers = {
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'X-ParlayPlay-Platform': 'web',
            'X-Parlay-Request': '1',
            'sec-ch-ua-mobile': '?0',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Referer': 'https://parlayplay.io/?coupon=PP5&utm_campaign=ppc_brand&gclid=CjwKCAiA75itBhA6EiwAkho9e0WKfb9Q9CfMPQX3pItGGIyHcVBKOkTekc8rapMRzs75GX8hNshKcxoCUXsQAvD_BwE',
            'X-Requested-With': 'XMLHttpRequest',
            'X-CSRFToken': '1',
            'sec-ch-ua-platform': '"macOS"',
        }

        response = cloudscraper.create_scraper().get(url, headers=headers)
        if response.status_code == 200:
            self.parse_lines(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response):
        data = response.json()
        last_updated = data.get('lastUpdated')
        if last_updated:
            last_updated = datetime.fromtimestamp(last_updated)

        for player in data.get('players', []):
            league, game_time, match = '', '', player.get('match')
            if match:
                player_league, game_time = match.get('league'), match.get('matchDate')
                if player_league:
                    league = player_league.get('leagueNameShort')

            subject, position, team, player_data = '', '', '', player.get('player')
            if player_data:
                subject, position = player_data.get('fullName'), player_data.get('position')
                team_data = player_data.get('team')
                if team_data:
                    team = team_data.get('teamAbbreviation')

            for stat in player.get('stats', []):
                is_boosted_payout, alt_lines = stat.get('isBoostedPayout'), stat.get('altLines')
                if alt_lines:
                    market = alt_lines.get('market')
                    for line in alt_lines.get('values', []):
                        stat_line = line.get('selectionPoints')
                        for label in ['Over', 'Under']:
                            odds = line.get('decimalPriceOver') if label == 'Over' else line.get('decimalPriceUnder')
                            if not odds:
                                continue

                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'game_time': game_time,
                                'market_category': 'player_props',
                                'market': market,
                                'subject_team': team,
                                'subject': subject,
                                'position': position,
                                'bookmaker': 'ParlayPlay',
                                'label': label,
                                'line': stat_line,
                                'odds': odds,
                                'is_boosted_payout': is_boosted_payout
                            })

        with open('parlayplay_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))


ParlayPlaySpider(batch_id="123-abc").start_requests()
