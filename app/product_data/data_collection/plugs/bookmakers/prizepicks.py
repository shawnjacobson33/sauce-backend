import re
from datetime import datetime
from typing import Optional

from bson import ObjectId

from app.product_data.data_collection.plugs.bookmakers import utils


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


def get_league(data: dict, leagues: dict) -> Optional[tuple[str, str]]:
    # get some dictionaries from relationship data, if both exist keep going
    if relationship_league := data.get('league'):
        # get another dictionary for relationship league data and a league id if they both exist.
        if (relationship_league_data := relationship_league.get('data')) and (league_id := relationship_league_data.get('id')):
            # the league id must exist in the leagues data store and also get the league name from the store
            if (league_id in leagues) and (league := leagues.get(league_id)):
                # clean league after extracting quarter or half info from it if it exists.
                cleaned_league = utils.clean_league(league[:-2] if re.match(r'^.+[1-4]([QH])$', league) else league)
                # only want valid leagues
                if utils.is_league_valid(cleaned_league):
                    # return the valid and cleaned league, and the original league name format without cleaning it (needed for market)
                    return league, cleaned_league

    return None, None


def extract_market(bookmaker_name: str, data: dict, uncleaned_league: str, cleaned_league: str) -> Optional[tuple[ObjectId, str]]:
    # get the market name and check for validity, if exists and valid then execute
    if (market_name := data.get('stat_type')) and utils.is_market_valid(market_name):
        # in order to create comparable market names -- for Quarter and Half Markets
        if re.match(r'^.+[1-4]([QH])$', uncleaned_league):
            # re-format the market name
            market_name = f'{uncleaned_league[-2:]} {market_name}'

        # create market object
        market_obj = utils.Market(market_name, cleaned_league)
        # gets the market id or log message
        market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position, with some logic for when a secondary position is included (ex: PG-SG)
        return utils.clean_position(position.split('-')[0] if '-' in position else position)


def extract_subject(bookmaker_name: str, data: dict, subjects_dict: dict, league: str) -> Optional[tuple[str, str]]:
    # get a dictionary that holds data on the player for the prop line, if both exist then execute
    if (relationship_new_player := data.get('new_player')) and (relationship_new_player_data := relationship_new_player.get('data')):
        # get the player id and then get the subject data that corresponds, if both exist then execute
        if (player_id := relationship_new_player_data.get('id')) and (subject_data := subjects_dict.get(str(player_id))):
            # get the subject name and check if it is a 'combo' player prop, if exists keep executing
            if (subject_name := subject_data.get('player_name')) and (' + ' not in subject_name):
                # get player attributes
                subject_team, position = subject_data.get('subject_team'), extract_position(subject_data)
                # create a subject object
                subject_obj = utils.Subject(subject_name, league, team=subject_team, position=position)
                # gets the subject id or log message
                subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
                # return both subject id search result and cleaned subject
                return subject_id, subject_name

    return None, None


class PrizePicks(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url required to make request for leagues data
        url = utils.get_url(self.bookmaker_info.name, name='leagues')
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
            url = utils.get_url(self.bookmaker_info.name)
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
                    if (prop_line_attrs := prop_line_data.get('attributes')) and (prop_line_attrs.get('odds_type') != 'standard'):
                        # get league data for prop lines
                        uncleaned_league, cleaned_league = get_league(relationships_data, leagues)
                        # if both exist keep executing
                        if uncleaned_league and cleaned_league:
                            # get the market id from db and extract market and league name from data, pass a generator to get the league
                            market_id, market_name = extract_market(self.bookmaker_info.name, prop_line_attrs, uncleaned_league, cleaned_league)
                            # if all three exist then keep executing
                            if market_id and market_name:
                                # to track the leagues being collected
                                self.metrics.add_league(cleaned_league)
                                # to track the markets being collected
                                self.metrics.add_market((cleaned_league, market_name))
                                # get the subject id from the db and extract the subject name
                                subject_id, subject_name = extract_subject(self.bookmaker_info.name, relationships_data, subjects_dict, cleaned_league)
                                # keep executing if both exist
                                if subject_id and subject_name:
                                    # to track the subjects being collected
                                    self.metrics.add_subject((cleaned_league, subject_name))
                                    # get numeric over/under line and check for existence
                                    if line := prop_line_attrs.get('line_score'):
                                        # for each generic label for an over/under line
                                        for label in ['Over', 'Under']:
                                            # update shared data
                                            self.update_betting_lines({
                                                'batch_id': self.batch_id,
                                                'time_processed': str(datetime.now()),
                                                'league': cleaned_league,
                                                'market_category': 'player_props',
                                                'market_id': str(market_id),
                                                'market': market_name,
                                                'subject_id': str(subject_id),
                                                'subject': subject_name,
                                                'bookmaker': self.bookmaker_info.name,
                                                'label': label,
                                                'line': line,
                                                'odds': self.bookmaker_info.default_payout.odds
                                            })


if __name__ == "__main__":
    utils.run(PrizePicks)
