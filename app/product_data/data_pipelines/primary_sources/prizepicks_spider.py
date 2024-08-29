import json
from datetime import datetime

import cloudscraper


class PrizePicksSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.scraper = cloudscraper.create_scraper()

    def start_requests(self):
        url = 'https://api.prizepicks.com/leagues?game_mode=pickem'

        # Get response.
        response = self.scraper.get(url)
        if response.status_code == 200:
            self.parse_leagues(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_leagues(self, response):
        leagues_data = response.json().get('data')

        # collect all the league ids
        leagues = dict()
        for league in leagues_data:
            league_name, attributes = '', league.get('attributes')
            if attributes:
                league_name = attributes.get('name')

                # don't want futures or leagues that aren't currently available
                if ('SZN' in league_name) or (not attributes.get('active')):
                    continue

            league_id = league.get('id')
            if league_id:
                leagues[league_id] = league_name

        url = "https://api.prizepicks.com/projections?"

        new_response = self.scraper.get(url)
        if new_response.status_code == 200:
            self.parse_lines(new_response, leagues)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response, leagues):
        data = response.json()

        # collect all the player ids
        players = dict()
        for player in data.get('included', []):
            if player.get('type') == 'new_player':
                player_id, player_attributes = player.get('id'), player.get('attributes')
                if player_id and player_attributes:
                    team, subject = player_attributes.get('team_name'), player_attributes.get('display_name')
                    if not team:
                        team = player_attributes.get('team')
                    if not subject:
                        player_attributes.get('name')

                    position = player_attributes.get('position')
                    players[player_id] = {'team': team, 'player_name': subject, 'position': position}

        # second pass will actually extract data from all the lines
        for line in data.get('data', []):
            last_updated, market, game_time, stat_line, line_attributes = '', '', '', '', line.get('attributes')
            if line_attributes:
                # for lines with multipliers ("demon" or "goblin") need to make a separate request to get the payout
                if line_attributes.get('odds_type') != 'standard':
                    continue

                last_updated, market = line_attributes.get('updated_at'), line_attributes.get('stat_type')
                game_time, stat_line = line_attributes.get('start_time'), line_attributes.get('line_score')

            league, relationships = '', line.get('relationships')
            if relationships:
                relationship_league = relationships.get('league')
                if relationship_league:
                    relationship_league_data = relationship_league.get('data')
                    if relationship_league_data:
                        league_id = relationship_league_data.get('id')
                        if league_id not in leagues:
                            continue

                        league = leagues.get(league_id)

                team, subject, position, relationship_new_player = '', '', '', relationships.get('new_player')
                if relationship_new_player:
                    relationship_new_player_data = relationship_new_player.get('data')
                    if relationship_new_player_data:
                        player_id = relationship_new_player_data.get('id')
                        player_data = players.get(str(player_id))
                        if player_data:
                            team, subject = player_data.get('team'), player_data.get('player_name')
                            position = player_data.get('position')

                for label in ['Over', 'Under']:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'last_updated': last_updated,
                        'league': league,
                        'market_category': 'player_props',
                        'market': market,
                        'game_time': game_time,
                        'team': team,
                        'subject': subject,
                        'position': position,
                        'bookmaker': "PrizePicks",
                        'label': label,
                        'line': stat_line
                    })

        with open('../data_samples/prizepicks_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))


PrizePicksSpider(batch_id="1234").start_requests()
