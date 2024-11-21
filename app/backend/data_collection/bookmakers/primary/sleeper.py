import asyncio
from datetime import datetime
from collections import defaultdict
from typing import Optional

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers import utils as bkm_utils


# formatting consistency
LEAGUE_MAP = {
    'NCAAF': 'CFB',
    'NCAAM': 'CBB'
}
# get in-season leagues
LEAGUES = [LEAGUE_MAP.get(league, league).lower() for league in dc_utils.IN_SEASON_LEAGUES if league != 'NCAAW']


def extract_league(data: dict) -> Optional[str]:
    # get the league from the data, if exists keep executing
    if league := data.get('sport'):
        # clean the league name
        cleaned_league = bkm_utils.clean_league(league)
        # check if the league name is valid
        if bkm_utils.is_league_valid(cleaned_league):
            # return the clean league name
            return cleaned_league


def extract_position(data: dict) -> Optional[str]:
    # get the player's position from the dictionary, if exists keep going
    if position := data.get('position'):
        # return the clean player position
        return bkm_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the player's team name from the dictionary
    if abbr_team_name := data.get('subject_team'):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team_id(bookmaker_name, league, abbr_team_name):
            # return the team id and team name
            return team_data


def extract_player_data(data: dict, league: str, players_dict: dict) -> Optional[dict]:
    # get the player's id and their data from dictionaries, if both exist keep executing
    if (player_id := data.get('subject_id')) and (league_data := players_dict.get(league)):
        # get some player data dictionary
        return league_data.get(player_id)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get the subject's name from the dictionary
    if subject_name := data.get('subject'):
        # get player attributes
        position = extract_position(data)
        # gets the subject id or log message
        subject = bkm_utils.get_subject(bookmaker_name, league, subject_name, team=team, position=position)
        # return both subject id search result and cleaned subject
        return subject


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the market name from the dictionary, if exists keep going
    if market_name := data.get('wager_type'):
        # gets the market id or log message
        market = bkm_utils.get_market_id(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_label(data: dict) -> Optional[str]:
    # get the label from the dictionary
    if label := data.get('outcome'):
        # return the label capitalized
        return label.title()

# TODO: Sleeper recently added alt_lines
class Sleeper(bkm_utils.LinesRetriever):
    def __init__(self, bookmaker: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)
        # get universally used headers to make requests
        self.headers = bkm_utils.get_headers(self.source.name)

    async def retrieve(self) -> None:
        # get the url required to request player data
        url = bkm_utils.get_url(self.source.name, name='players')
        # get params required to request player data
        params = bkm_utils.get_params(self.source.name)
        # make the request for player data
        await self.req_mngr.get(url, self._parse_players, headers=self.headers, params=params)

    async def _parse_players(self, response) -> None:
        # get the response data as json, if exists then keep executing
        if json_data := response.json():
            # initialize a dictionary to hold subject data
            players_dict = defaultdict(dict)
            # for each player's data in the json
            for player_data in json_data:
                # get the subject team and player id, if both exists then keep executing
                if (subject_team := player_data.get('team')) and (player_id := player_data.get('player_id')):
                    # get subject's first and last name, if both exist then keep executing
                    if (first_name := player_data.get('first_name')) and (last_name := player_data.get('last_name')):
                        # get the subject's sport, if exists then keep executing
                        if subject_sport := player_data.get('sport'):
                            cleaned_league = bkm_utils.clean_league(subject_sport)
                            if bkm_utils.is_league_valid(cleaned_league):
                                # store data about the subject, corresponding to their sport and id
                                players_dict[cleaned_league][player_id] = {
                                    'subject_team': subject_team,
                                    'subject': ' '.join([first_name, last_name]),
                                    'position': player_data.get('position')
                                }

            # create a list to store requests to make
            tasks = list()
            # for each in season league mapped towards sleeper's format
            for league_name in LEAGUES:
                # get the url required to make request for prop lines
                url = bkm_utils.get_url(self.source.name)
                # create a dictionary of params specific to a particular league
                params = {
                    'sports%5B%5D': league_name,
                    'dynamic': 'true',
                    'include_preseason': 'true',
                    'eg': '18.control'
                }
                # add the request task to the list
                tasks.append(self.req_mngr.get(url, self._parse_lines, players_dict, headers=self.headers, params=params))
                # Sleeper also offers alt_lines...make separate request for those
                alt_url = url.replace('available', 'available_alt')
                # delete some parameters that aren't used in typical requests to this endpoint
                del params['dynamic']
                del params['eg']
                # add the request for alt prop lines data to the list
                tasks.append(self.req_mngr.get(alt_url, self._parse_lines, players_dict, headers=self.headers, params=params))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, players: dict) -> None:
        # get response data as json, if exists keep going
        if json_data := response.json():
            # for each dictionary of data in response
            for prop_line_data in json_data:
                # extract the league name from dictionary
                if league := extract_league(prop_line_data):
                    # to track the leagues being collected
                    bkm_utils.Leagues.update_valid_leagues(self.source.name, league)
                    # get the nested player data
                    if player_data := extract_player_data(prop_line_data, league, players):
                        # get the team from the player data
                        team = extract_team(self.source.name, league, player_data)
                        # get the game data from database
                        if game := bkm_utils.get_game_id(team):
                            # get the subject id from db and extract player name from dictionary
                            if subject := extract_subject(self.source.name, player_data, league, team):
                                # get the market id from db and extract the market name from dictionary
                                if market := extract_market(self.source.name, prop_line_data, league):
                                    # for each dictionary containing label, line, odds in prop_line_data's options if exists
                                    for outcome_data in prop_line_data.get('options', []):
                                        # get the numeric over/under line and the decimal odds from the dictionary, if both exist keep going
                                        if (line := outcome_data.get('outcome_value')) and (odds := outcome_data.get('payout_multiplier')):
                                            # get the over or under label from the dictionary, if exists keep going
                                            if label := extract_label(outcome_data):
                                                # update shared data
                                                self.update_betting_lines({
                                                    'batch_id': self.batch_id,
                                                    'time_processed': datetime.now(),
                                                    'league': league,
                                                    'game_id': game['id'],
                                                    'game': game['info'],
                                                    'market_category': 'player_props',
                                                    'market_id': market['id'],
                                                    'market': market['name'],
                                                    'subject_id': subject['id'],
                                                    'subject': subject['name'],
                                                    'bookmaker': self.source.name,
                                                    'label': label,
                                                    'line': line,
                                                    'odds': float(odds)
                                                })
