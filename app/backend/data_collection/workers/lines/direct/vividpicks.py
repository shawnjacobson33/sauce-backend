from datetime import datetime
from typing import Optional

from app.backend.data_collection.workers import utils as dc_utils
from app.backend.data_collection.workers.lines import utils as ln_utils


def extract_game_info(data: dict) -> Optional[str]:
    # get the game info and check if this isn't a Futures prop line, if so then keep executing
    if (game_info := data.get('gameInfo')) and ('Futures' not in game_info):
        # return the game info
        return game_info


def extract_league(data: dict) -> Optional[str]:
    # get the league name from dictionary, if it exists keep executing
    if league := data.get('league'):
        # clean the league name
        cleaned_league = dc_utils.clean_league(league)
        # check if the league name is valid
        if ln_utils.is_league_valid(cleaned_league):
            # return the cleaned and valid league name
            return cleaned_league


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the team name from the dictionary
    if abbr_team_name := data.get('abvTeamName'):
        # store an abbreviated team name tuple
        team_name = abbr_team_name
        # if it doesn't exist and there is a full team name available
        if not abbr_team_name and (full_team_name := data.get('teamName')):
            # update the team name tuple with the full team name
            team_name = ('full_name', full_team_name)

        # return the team id and team name
        return dc_utils.get_team(bookmaker_name, league, team_name)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get the player's name, if it exists keep going
    if subject_name := data.get('name'):
        # return both subject id search result and cleaned subject
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


def extract_multiplier(data: dict, market: str) -> Optional[float]:
    # get some dictionaries that hold data about the multiplier, if both exist keep executing
    if (m_player_props_data := data.get('configPlayerProps')) and (m_market_data := m_player_props_data.get(market)):
        # return the multiplier cast from a str to float
        return float(m_market_data.get('multiplier', 1.0))

    return 1.0


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the market name from the dictionary, if exists keep going
    if market_name := data.get('p'):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def get_labels(multiplier: Optional[float]) -> list:
    # generic labels for prop line
    labels = ['Over', 'Under']
    # must also exist to compare with an int type
    if multiplier:
        # checks if it is less than 1
        if multiplier < 1:
            # returns only 'Under'
            return [labels[1]]
        # checks if it is over 1
        elif multiplier > 1:
            # returns only 'Over'
            return [labels[0]]

    # otherwise just return both labels because multiplier is 1
    return labels


def get_odds(default_odds: float, multiplier: Optional[float]) -> float:
    # multiplier must exist to multiply with float type
    if multiplier:
        # return the product of the bookmaker's default odds stored in the db with the multiplier to get adjusted odds
        return round(default_odds * multiplier, 3)

    # otherwise the default-best odds available will just be used
    return default_odds


class VividPicks(ln_utils.LinesRetriever):
    def __init__(self, bookmaker: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)

    async def retrieve(self) -> None:
        # get the url required to request for prop lines data
        url = ln_utils.get_url(self.name)
        # get the headers required to request for prop lines data
        headers = ln_utils.get_headers(self.name)
        # get the json required to request for prop lines data
        json_data = ln_utils.get_json_data(self.name, )
        # make the request for prop lines data
        await self.req_mngr.post(url, self._parse_lines, headers=headers, json=json_data)

    async def _parse_lines(self, response) -> None:
        # get the response data as json, execute if it exists
        if json_data := response.json():
            # for each dictionary in the response data's gret if it exists
            for event_data in json_data.get('gret', []):
                # extract the league name from dictionary, if it exists keep going
                if league := extract_league(event_data):
                    # get the sport for this league
                    sport = dc_utils.LEAGUE_SPORT_MAP[league]
                    # to track the leagues being collected
                    dc_utils.RelevantData.update_relevant_leagues(league, self.name)
                    # for each dictionary in event data's activePlayers if they exist
                    for player_data in event_data.get('activePlayers', []):
                        # get player attributes
                        if team := extract_team(self.name, league, player_data):
                            # get the game data from database
                            if game := dc_utils.get_game(league, team['abbr_name']):
                                # get the subject id from db and extract the subject name from dictionary
                                if subject := extract_subject(self.name, player_data, league, team):
                                    # for each dictionary in player data's visiblePlayerProps if they exist
                                    for prop_line_data in player_data.get('visiblePlayerProps', []):
                                        # get the market id from the db and extract the market name from the dictionary
                                        if market := extract_market(self.name, prop_line_data, league):
                                            # get the numeric over/under line from the dictionary, keep going if exists
                                            if line := prop_line_data.get('val'):
                                                # extract the multiplier from the dictionary
                                                multiplier = extract_multiplier(player_data, market['name'])
                                                # for each label (depending on the value of the multiplier)
                                                for label in get_labels(multiplier):
                                                    # get the odds
                                                    if odds := get_odds(self.dflt_odds, multiplier):
                                                        # update shared data
                                                        dc_utils.BettingLines.update({
                                                            's_tstamp': str(datetime.now()),
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
                                                            'mult': multiplier,
                                                            'odds': odds,
                                                            'im_prb': round(1 / odds, 4)
                                                        })
