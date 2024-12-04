import asyncio
from collections import deque
from datetime import datetime
from typing import Optional, Any, Union

from backend.app.data_collection.workers import utils as dc_utils
from backend.app.data_collection.workers.lines import utils as ln_utils


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
            if ln_utils.is_league_valid(league):
                # return the cleaned league name
                return dc_utils.clean_league(league)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get 3 different dictionaries holding market data, if all exist keep going
    if (choices := data.get('choices')) and (actor := choices[0].get('actor')):
        if requirements := actor.get('winningRequirement'):
            # get the market name from the dictionary, if exists keep going
            if market_name := requirements[0].get('name'):
                # gets the market id or log message
                market = dc_utils.get_market(bookmaker_name, league, market_name)
                # return both market id search result and cleaned market
                return market


def extract_position(data: dict) -> Optional[str]:
    # get the abbreviated player's position, if exists keep going
    if position := data.get('posAbbr'):
        # return the cleaned player's position
        return dc_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[tuple[str, str]]:
    # get the player's team name from the dictionary
    if abbr_team_name := data.get('teamAbbr'):
        # return the team id and team name
        return dc_utils.get_team(bookmaker_name, league, abbr_team_name)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: str) -> dict[str, Union[Optional[dict[str, str]], Any]]:
    # get the first and last name of the player, if both exist keep going
    if (first_name := data.get('fName')) and (first_name != 'combined') and (last_name := data.get('lName')):
        # get subject name
        subject_name = ' '.join([first_name, last_name])
        # gets the subject id or log message
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


class SuperDraft(ln_utils.LinesRetriever):
    def __init__(self, batch_id: str, bookmaker: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(batch_id, bookmaker)
        # get the headers required to request prop lines data
        self.headers = ln_utils.get_headers(self.name)
        # update headers timestamp
        self.headers['timestamp'] = str(datetime.now())

    async def retrieve(self) -> None:
        # get the url required to request prop lines data
        url = ln_utils.get_url(self.name).format('0')
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
                    cleaned_league = dc_utils.clean_league(league_name)
                    # if it is a valid league and has props available
                    if ln_utils.is_league_valid(cleaned_league) and sport_data.get('hasProps'):
                        # get the league id if exists
                        if league_id := sport_data.get('sportId'):
                            # get the url with the inserted sport id param
                            url = ln_utils.get_url(self.name).format(league_id)
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
                    # get the sport for this league
                    sport = dc_utils.LEAGUE_SPORT_MAP[league]
                    # to track the leagues being collected
                    dc_utils.RelevantData.update_relevant_leagues(league, self.name)
                    # get the market id and extract the market name from the dictionary
                    if market := extract_market(self.name, prop_line_data, league):
                        # get player data dictionary, if exists keep going
                        if player_data := prop_line_data.get('player'):
                            # get some team data
                            if team_id := extract_team(self.name, league, player_data):
                                # get the game data from database
                                if game := dc_utils.get_game(team_id):
                                    # get the subject id from the db and extract the subject name from the dictionary
                                    if subject := extract_subject(self.name, player_data, league, team_id[1]):
                                        # get the numeric over/under line from the dictionary
                                        if line := prop_line_data.get('line'):
                                            # for each generic over/under label for prop lines
                                            for label in ['Over', 'Under']:
                                                # update shared data
                                                self.store({
                                                    'bookmaker': self.name,
                                                    'sport': sport,
                                                    'league': league,
                                                    'game_time': game['game_time'],
                                                    'game': game['info'],
                                                    'market_id': market['id'],
                                                    'market': market['name'],
                                                    'subject_id': subject['id'],
                                                    'subject': subject['name'],
                                                    'label': label,
                                                    'line': line,
                                                    'dflt_odds': self.dflt_odds,
                                                    'dflt_im_prb': self.dflt_im_prb
                                                })
