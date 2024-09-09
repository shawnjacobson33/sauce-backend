import asyncio
import json
import os
import re
import time
import uuid
from datetime import datetime
from pymongo import MongoClient

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper


class PrizePicksSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='PrizePicks')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url(name='leagues')
        # Get response.
        await self.rm.get(url, self._parse_leagues)

    async def _parse_leagues(self, response):
        leagues_data = response.json().get('data')
        # collect all the league ids
        leagues = dict()
        for league in leagues_data:
            league_name, attributes = None, league.get('attributes')
            if attributes:
                league_name = attributes.get('name')

                # don't want futures or leagues that aren't currently available
                if ('SZN' in league_name) or ('SPECIALS' in league_name) or (not attributes.get('active')):
                    continue

            league_id = league.get('id')
            if league_id:
                leagues[league_id] = league_name

        url = self.helper.get_url()
        await self.rm.get(url, self._parse_lines, leagues)

    async def _parse_lines(self, response, leagues):
        data = response.json()
        # collect all the player ids
        players = dict()
        for player in data.get('included', []):
            if player.get('type') == 'new_player':
                player_id, player_attributes = player.get('id'), player.get('attributes')
                if player_id and player_attributes:
                    subject_team, subject = player_attributes.get('team'), player_attributes.get('display_name')
                    if not subject_team:
                        subject_team = player_attributes.get('team_name')
                    if not subject:
                        player_attributes.get('name')

                    position = player_attributes.get('position')
                    players[player_id] = {'subject_team': subject_team, 'player_name': subject, 'position': position}

        # second pass will actually extract data from all the lines
        for line in data.get('data', []):
            league, relationships = None, line.get('relationships')
            if relationships:
                relationship_league = relationships.get('league')
                if relationship_league:
                    relationship_league_data = relationship_league.get('data')
                    if relationship_league_data:
                        league_id = relationship_league_data.get('id')
                        if league_id not in leagues:
                            continue

                        league = leagues.get(league_id)
                        if league:
                            league = DataCleaner.clean_league(league)

            subject_id = None
            subject_team, subject, position, relationship_new_player = None, None, None, relationships.get('new_player')
            if relationship_new_player:
                relationship_new_player_data = relationship_new_player.get('data')
                if relationship_new_player_data:
                    player_id = relationship_new_player_data.get('id')
                    player_data = players.get(str(player_id))
                    if player_data:
                        subject_team, subject = player_data.get('subject_team'), player_data.get('player_name')
                        position = player_data.get('position')
                        if subject:
                            subject = subject.strip()
                            subject_id = self.dn.get_subject_id(subject, league, subject_team, position)

            market_id = None
            last_updated, market, game_time, stat_line, line_attributes = None, None, None, None, line.get('attributes')
            if line_attributes:
                # for lines with multipliers ("demon" or "goblin") need to make a separate request to get the payout
                if line_attributes.get('odds_type') != 'standard':
                    continue

                last_updated, market = line_attributes.get('updated_at'), line_attributes.get('stat_type')
                if market:
                    # don't want combos...hard to index
                    if '(Combo)' in market:
                        continue

                    # in order to create more distinct markets
                    if 'Fantasy Score' in market:
                        if league in {'NBA', 'WNBA', 'WNBA1H', 'WNBA2H', 'WNBA1Q', 'WNBA2Q', 'WNBA3Q', 'WNBA4Q', 'WNBA1H'}:
                            market = 'Basketball Fantasy Score'
                        elif league == 'TENNIS':
                            market = 'Tennis Fantasy Score'
                        elif league in {'NFL', 'NFL1Q', 'NFL2Q', 'NFL3Q', 'NFL4Q', 'NFL1H', 'NFL2H', 'NCAAF', 'NCAAF1Q'}:
                            market = 'Football Fantasy Score'
                        elif league in {'INDYCAR', 'NASCAR'}:
                            market = 'Car Racing Fantasy Score'
                        elif league in {'MMA', 'UFC'}:
                            market = 'Fighting Fantasy Score'
                        elif league == 'MLB':
                            market = 'Baseball Fantasy Score'

                    # in order to create comparable market names
                    if re.match(r'^.+[1-4]([QH])$', league):
                        market = f'{league[-2:]} {market}'
                        league = league[:-2]

                    if market:
                        market_id = self.dn.get_market_id(market)

                game_time, stat_line = line_attributes.get('start_time'), line_attributes.get('line_score')
                for label in ['Over', 'Under']:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'last_updated': last_updated,
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market_name': market,
                        'game_time': game_time,
                        'subject_team': subject_team,
                        'position': position,
                        'subject_id': subject_id,
                        'subject': subject,
                        'bookmaker': "PrizePicks",
                        'label': label,
                        'line': stat_line
                    })

        self.helper.store(self.prop_lines)


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')
    db = client['sauce']
    spider = PrizePicksSpider(uuid.uuid4(), RequestManager(), DataNormalizer('PrizePicks', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[PrizePicks]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
