import asyncio
import re
from datetime import datetime
from typing import Optional, Iterable

from app.product_data.data_collection.plugs.bookmakers import utils
from app.product_data.data_collection.utils import standardizing as std
from app.product_data.data_collection.plugs.bookmakers.utils import setup
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug
from app.product_data.data_collection.utils.objects import Subject, Market, Bookmaker


def extract_league_for_leagues(data: dict) -> Optional[str]:
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
                if subject := player_attributes.get('display_name', player_attributes.get('name')):
                    # store the player id corresponding to some of the subject's attributes
                    players_dict[player_id] = {
                        'subject': subject,
                        'subject_team': player_attributes.get('team', player_attributes.get('team_name')),
                        'position': player_attributes.get('position')
                    }

    # return the dictionary with the player's info and ids
    return players_dict


def extract_league_for_lines(data: dict, leagues: dict) -> Optional[str]:
    # get some dictionaries from relationship data, if both exist keep going
    if relationship_league := data.get('league'):
        # get another dictionary for relationship league data and a league id if they both exist.
        if (relationship_league_data := relationship_league.get('data')) and (league_id := relationship_league_data.get('id')):
            # the league id must exist in the leagues data store and also get the league name from the store
            if (league_id in leagues) and (league := leagues.get(league_id)):
                # yield the original league name format without cleaning it (needed for market)
                yield league
                # clean league after extracting quarter or half info from it if it exists.
                cleaned_league = clean_league(league[:-2] if re.match(r'^.+[1-4]([QH])$', league) else league)
                # only want valid leagues
                if is_league_valid(cleaned_league):
                    # yield the valid and cleaned league
                    yield cleaned_league


def extract_market_and_league(data: dict, league_generator) -> Optional[tuple[Union[ObjectId, str], str]]:
    # get the unformatted league, if exists then keep executing
    if league := next(league_generator):
        # get the market name and check for validity, if exists and valid then execute
        if (market := data.get('stat_type')) and is_market_valid(market):
            # in order to create comparable market names -- for Quarter and Half Markets
            if re.match(r'^.+[1-4]([QH])$', league):
                # re-format the market name
                market = f'{league[-2:]} {market}'

            # get the cleaned and valid league name
            if cleaned_league := next(league_generator):
                # clean the market name
                cleaned_market = clean_market(market)
                # return the market id and cleaned market name
                return get_market_id(Market(cleaned_market, cleaned_league)), cleaned_market, cleaned_league


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position, with some logic for when a secondary position is included (ex: PG-SG)
        return clean_position(position.split('-')[0] if '-' in position else position)


def extract_subject(data: dict, subjects_dict: dict, league: str) -> Optional[tuple[str, str]]:
    # get a dictionary that holds data on the player for the prop line, if both exist then execute
    if (relationship_new_player := data.get('new_player')) and (relationship_new_player_data := relationship_new_player.get('data')):
        # get the player id and then get the subject data that corresponds, if both exist then execute
        if (player_id := relationship_new_player_data.get('id')) and (subject_data := subjects_dict.get(str(player_id))):
            # get the subject name and check if it is a 'combo' player prop, if exists keep executing
            if (subject := subject_data.get('player_name')) and (' + ' not in subject):
                # clean the subject name
                cleaned_subject = clean_subject(subject)
                # return the subject id from the db and the cleaned subject name
                return get_subject_id(Subject(subject, league, subject_data.get('subject_team'), extract_position(subject_data))), cleaned_subject


class PrizePicks(BookmakerPlug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def collect(self) -> None:
        # get the url required to make request for leagues data
        url = utils.get_url(name='leagues')
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
                if (league := extract_league_for_leagues(league_data)) and (league_id := league_data.get('id')):
                    # store the league id with corresponding league name
                    leagues[league_id] = league
                    # get the url required to make request for prop lines
                    url = utils.get_url()
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
                        # get the market id from db and extract market and league name from data, pass a generator to get the league
                        market_id, market, league = extract_market_and_league(prop_line_attrs, extract_league_for_lines(relationships_data, leagues))
                        # if all three exist then keep executing
                        if market_id and market and league:
                            # get the subject id from the db and extract the subject name
                            subject_id, subject, message = extract_subject(relationships_data, subjects_dict, league)
                            # keep executing if both exist, and get numeric over/under line and check for existence
                            if subject_id and subject and (line := prop_line_attrs.get('line_score')):
                                # for each generic label for an over/under line
                                for label in ['Over', 'Under']:
                                    # update shared data
                                    self.add_and_update({
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
                                        'line': line,
                                        'odds': self.info.default_payout.odds
                                    })


if __name__ == "__main__":
    asyncio.run(run(PrizePicks))
