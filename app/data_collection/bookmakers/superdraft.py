import asyncio
from datetime import datetime
from typing import Optional, Any, Union

from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import utils as bkm_utils
from app.data_collection.bookmakers.to_do.unabated import headers


def extract_sports_dict(data: dict) -> dict:
    # initialize a dictionary to hold the sports id and name data
    sports_dict = dict()
    # for each sport dictionary in data's sports if they exist
    for sport_data in data.get('sports', []):
        # get the sport id and sport name from the dictionary, if both exist keep going
        if (sport_id := sport_data.get('sportId')) and (sport_name := sport_data.get('sName')):
            # store the sport id with its corresponding name
            sports_dict[sport_id] = sport_name

    # return the dictionary of data
    return sports_dict


def extract_league(data: dict, sports_dict: dict) -> Optional[str]:
    # only wanting individual subject prop lines
    if data.get('type') != 'matchup-prop':
        # get the sport id and league from dictionaries, if both exist then keep going
        if (sport_id := data.get('sportId')) and (league := sports_dict.get(int(sport_id))):
            # check if league name is valid
            if bkm_utils.is_league_valid(league):
                # return the cleaned league name
                return bkm_utils.clean_league(league)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get 3 different dictionaries holding market data, if all exist keep going
    if (choices := data.get('choices')) and (actor := choices[0].get('actor')):
        if requirements := actor.get('winningRequirement'):
            # get the market name from the dictionary, if exists keep going
            if market_name := requirements[0].get('name'):
                # gets the market id or log message
                market = bkm_utils.get_market_id(bookmaker_name, league, market_name)
                # return both market id search result and cleaned market
                return market


def extract_position(data: dict) -> Optional[str]:
    # get the abbreviated player's position, if exists keep going
    if position := data.get('posAbbr'):
        # return the cleaned player's position
        return bkm_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the player's team name from the dictionary
    if team_name := data.get('teamAbbr'):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team_id(bookmaker_name, league, team_name):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str) -> dict[str, Union[Optional[dict[str, str]], Any]]:
    # get player data dictionary, if exists keep going
    if player_data := data.get('player'):
        # get the first and last name of the player, if both exist keep going
        if (first_name := player_data.get('fName')) and (first_name != 'combined') and (last_name := player_data.get('lName')):
            # get some game info
            game_info = player_data.get('eventName')
            # get subject name
            subject_name = ' '.join([first_name, last_name])
            # get player attributes
            team, position = extract_team(bookmaker_name, league, player_data), extract_position(player_data)
            # gets the subject id or log message
            if subject := bkm_utils.get_subject_id(bookmaker_name, league, subject_name, team=team, position=position):
                # return both subject id search result and cleaned subject
                return {
                    'subject': subject,
                    'game_info': game_info
                }


class SuperDraft(bkm_utils.BookmakerPlug):
    def __init__(self, bookmaker_info: bkm_utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)
        # get the headers required to request prop lines data
        self.headers = bkm_utils.get_headers(self.bookmaker_info.name)
        # update headers timestamp
        self.headers['timestamp'] = str(datetime.now())

    async def collect(self) -> None:
        # get the url required to request prop lines data
        url = bkm_utils.get_url(self.bookmaker_info.name).format('0')
        # make the request for the prop lines
        await self.req_mngr.get(url, self._parse_leagues, headers=self.headers)

    async def _parse_leagues(self, response) -> None:
        # get response data in json, if exists then keep going
        if json_data := response.json():
            # initialize a structure to hold requests to make
            tasks = list()
            # for each sports data dictionary
            for sport_data in json_data.get('sports', []):
                # get the league name
                if league_name := sport_data.get('sName'):
                    # clean the league name
                    cleaned_league = bkm_utils.clean_league(league_name)
                    # if it is a valid league and has props available
                    if bkm_utils.is_league_valid(cleaned_league) and sport_data.get('hasProps'):
                        # get the league id if exists
                        if league_id := sport_data.get('sportId'):
                            # get the url with the inserted sport id param
                            url = bkm_utils.get_url(self.bookmaker_info.name).format(league_id)
                            # add the request for the prop lines
                            tasks.append(self.req_mngr.get(url, self._parse_lines, headers=self.headers))

            # asynchronously request prop lines for all leagues
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response) -> None:
        # get response data in json, if exists then keep going
        if json_data := response.json():
            # get sports ids
            sports_dict = extract_sports_dict(json_data)
            # for each dictionary of prop line data in json_data's props if they exist
            for prop_line_data in json_data.get('props', []):
                # extract the league name from the dictionary, if exists keep going
                if league := extract_league(prop_line_data, sports_dict):
                    # to track the leagues being collected
                    bkm_utils.Leagues.update_valid_leagues(self.bookmaker_info.name, league)
                    # get the market id and extract the market name from the dictionary
                    if market := extract_market(self.bookmaker_info.name, prop_line_data, league):
                        # get the subject id from the db and extract the subject name from the dictionary
                        if subject := extract_subject(self.bookmaker_info.name, prop_line_data, league):
                            # get the numeric over/under line from the dictionary
                            if line := prop_line_data.get('line'):
                                # for each generic over/under label for prop lines
                                for label in ['Over', 'Under']:
                                    # update shared data
                                    self.update_betting_lines({
                                        'batch_id': self.batch_id,
                                        'time_processed': str(datetime.now()),
                                        'league': league,
                                        'game_info': subject['game_info'],
                                        'market_category': 'player_props',
                                        'market_id': market['id'],
                                        'market': market['name'],
                                        'subject_id': subject['subject']['id'],
                                        'subject': subject['subject']['name'],
                                        'bookmaker': self.bookmaker_info.name,
                                        'label': label,
                                        'line': line,
                                        'odds': self.bookmaker_info.default_payout.odds
                                    })
