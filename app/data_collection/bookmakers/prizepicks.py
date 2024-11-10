import re
from datetime import datetime
from typing import Optional

from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import utils as bkm_utils


def extract_league(data: dict) -> Optional[str]:
    # get the attributes that hold league data along with the league name, if both exist
    if (attributes := data.get('attributes')) and (league := attributes.get('name')):
        # don't want futures or leagues that aren't currently available, check if this league is active
        if ('SZN' not in league) and ('SPECIALS' not in league) and attributes.get('active'):
            # return the league name
            return league


def extract_subjects_dict(data: dict) -> dict:
    # initialize a players dictionary to hold player ids and player attributes
    players_dict = dict()
    # for each player in a dictionary of data
    for player_data in data.get('included', []):
        # if the player data type is valid
        if player_data.get('type') == 'new_player':
            # get the player's id and attributes, if both exist keep going
            if (player_id := player_data.get('id')) and (player_attributes := player_data.get('attributes')):
                # get the subject name, if first doesn't exist then get other format
                if subject_name := player_attributes.get('display_name', player_attributes.get('name')):
                    # store the player id corresponding to some of the subject's attributes
                    players_dict[player_id] = {
                        'subject': subject_name,
                        'subject_team': player_attributes.get('team', player_attributes.get('team_name')),
                        'position': player_attributes.get('position')
                    }

    # return the dictionary with the player's info and ids
    return players_dict


def get_league(data: dict, leagues: dict) -> Optional[dict[str, str]]:
    # get some dictionaries from relationship data, if both exist keep going
    if relationship_league := data.get('league'):
        # get another dictionary for relationship league data and a league id if they both exist.
        if (relationship_league_data := relationship_league.get('data')) and (league_id := relationship_league_data.get('id')):
            # the league id must exist in the leagues data store and also get the league name from the store
            if league_name := leagues.get(league_id):
                # clean league after extracting quarter or half info from it if it exists.
                cleaned_league = bkm_utils.clean_league(league_name[:-2] if re.match(r'^.+[1-4]([QH])$', league_name) else league_name)
                # only want valid leagues
                if bkm_utils.is_league_valid(cleaned_league):
                    # return the valid and cleaned league, and the original league name format without cleaning it (needed for market)
                    return {
                        'uncleaned': league_name,
                        'cleaned': cleaned_league
                    }


def extract_market(bookmaker_name: str, data: dict, league: dict) -> Optional[dict[str, str]]:
    # get the market name and check for validity, if exists and valid then execute
    if (market_name := data.get('stat_type', data.get('stat_display_name'))) and bkm_utils.is_market_valid(market_name):
        # in order to create comparable market names -- for Quarter and Half Markets
        if re.match(r'^.+[1-4]([QH])$', league['uncleaned']):
            # re-format the market name
            market_name = f'{league["uncleaned"][-2:]} {market_name}'

        # gets the market id or log message
        market = bkm_utils.get_market_id(bookmaker_name, league['cleaned'], market_name)
        # return both market id search result and cleaned market
        return market


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position, with some logic for when a secondary position is included (ex: PG-SG)
        return bkm_utils.clean_position(position.split('-')[0] if '-' in position else position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the player's team name from the dictionary
    if team_name := data.get('subject_team'):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team_id(bookmaker_name, league, team_name):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, subjects_dict: dict, league: str) -> Optional[dict[str, str]]:
    # get a dictionary that holds data on the player for the prop line, if both exist then execute
    if (relationship_new_player := data.get('new_player')) and (relationship_new_player_data := relationship_new_player.get('data')):
        # get the player id and then get the subject data that corresponds, if both exist then execute
        if (player_id := relationship_new_player_data.get('id')) and (subject_data := subjects_dict.get(str(player_id))):
            # get the subject name and check if it is a 'combo' player prop, if exists keep executing
            if (subject_name := subject_data.get('subject')) and (' + ' not in subject_name):
                # get player attributes
                team, position = extract_team(bookmaker_name, league, subject_data), extract_position(subject_data)
                # gets the subject id or log message
                subject = bkm_utils.get_subject_id(bookmaker_name, league, subject_name, team=team, position=position)
                # return both subject id search result and cleaned subject
                return subject


class PrizePicks(bkm_utils.BookmakerPlug):
    def __init__(self, bookmaker_info: bkm_utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url required to make request for leagues data
        url = bkm_utils.get_url(self.bookmaker_info.name, name='leagues')
        # make request for leagues data
        await self.req_mngr.get(url, self._parse_leagues)

    async def _parse_leagues(self, response) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # initialize a dictionary to hold league data
            leagues = dict()
            # for each league in the list of league dicts
            for league_data in data:
                # get the league and league id from dictionary, if both exist keep going
                if (league := extract_league(league_data)) and (league_id := league_data.get('id')):
                    # store the league id with corresponding league name
                    leagues[league_id] = league

            # get the url required to make request for prop lines
            url = bkm_utils.get_url(self.bookmaker_info.name)
            # make the request for prop lines
            await self.req_mngr.get(url, self._parse_lines, leagues)

    async def _parse_lines(self, response, leagues: dict) -> None:
        # gets the json data from the response, if exists then keep going
        if json_data := response.json():
            # get a dictionary storing subject ids and corresponding subject attributes
            subjects_dict = extract_subjects_dict(json_data)
            # second pass will actually extract data from all the lines
            for prop_line_data in json_data.get('data', []):
                # get a dictionary that holds data on prop line attributes
                if relationships_data := prop_line_data.get('relationships'):
                    # get some prop line attributes and only get prop lines that aren't alt (takes separate requests)
                    if (prop_line_attrs := prop_line_data.get('attributes')) and (prop_line_attrs.get('odds_type') == 'standard'):
                        # get league data for prop lines
                        if league := get_league(relationships_data, leagues):
                            # get the market id from db and extract market and league name from data, pass a generator to get the league
                            if market := extract_market(self.bookmaker_info.name, prop_line_attrs, league):
                                # to track the leagues being collected
                                bkm_utils.Leagues.update_valid_leagues(self.bookmaker_info.name, league['cleaned'])
                                # get the subject id from the db and extract the subject name
                                if subject := extract_subject(self.bookmaker_info.name, relationships_data, subjects_dict, league['cleaned']):
                                    # get numeric over/under line and check for existence
                                    if line := prop_line_attrs.get('line_score'):
                                        # for each generic label for an over/under line
                                        for label in ['Over', 'Under']:
                                            # update shared data
                                            self.update_betting_lines({
                                                'batch_id': self.batch_id,
                                                'time_processed': str(datetime.now()),
                                                'league': league['cleaned'],
                                                'market_category': 'player_props',
                                                'market_id': market['id'],
                                                'market': market['name'],
                                                'subject_id': subject['id'],
                                                'subject': subject['name'],
                                                'bookmaker': self.bookmaker_info.name,
                                                'label': label,
                                                'line': line,
                                                'odds': self.bookmaker_info.default_payout.odds
                                            })
