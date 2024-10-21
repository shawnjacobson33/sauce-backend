import asyncio
import re
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.constants import FANTASY_SCORE_MAP
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class PrizePicks(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url(name='leagues')
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

        url = self.packager.get_url()
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
        subject_ids = dict()
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

            market_id = None
            last_updated, market, stat_line, line_attributes = None, None, None, line.get('attributes')
            if line_attributes:
                # for lines with multipliers ("demon" or "goblin") need to make a separate request to get the payout
                if line_attributes.get('odds_type') != 'standard':
                    continue

                last_updated, market = line_attributes.get('updated_at'), line_attributes.get('stat_type')
                if market:
                    # don't want combos...hard to index for now
                    if '(Combo)' in market:
                        continue

                    # in order to create more distinct markets
                    if 'Fantasy Score' in market:
                        market = FANTASY_SCORE_MAP.get(league, market)

                    # in order to create comparable market names -- for Quarter and Half Markets
                    if re.match(r'^.+[1-4]([QH])$', league):
                        market = f'{league[-2:]} {market}'
                        league = league[:-2]

                    # clean league after extracting quarter or half info from it if it exists.
                    if league:
                        league = clean_league(league)
                        if not Packager.is_league_good(league):
                            continue

                    if market:
                        market = clean_market(market)
                        market_id = self.ds.get_market_id(Market(market, league))

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
                        # for players with secondary positions
                        if position and '-' in position:
                            position = clean_position(position.split('-')[0])

                        if subject:
                            # don't want combo props
                            if ' + ' in subject:
                                continue

                            subject = clean_subject(subject)
                            subject_id = subject_ids.get(f'{subject}{subject_team}')
                            if not subject_id:
                                subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                                subject_ids[f'{subject}{subject_team}'] = subject_id

                stat_line = line_attributes.get('line_score')
                for label in ['Over', 'Under']:
                    PropLines.update(''.join(self.info.name.split()).lower(), {
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'subject_id': subject_id,
                        'subject': subject,
                        'bookmaker': self.info.name,
                        'label': label,
                        'line': stat_line,
                        'odds': self.info.default_payout.odds
                    })
                    self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(PrizePicks))
