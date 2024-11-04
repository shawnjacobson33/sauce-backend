import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils
from app.product_data.data_collection.utils import standardizing as std
from app.product_data.data_collection.plugs.bookmakers.utils import setup
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug
from app.product_data.data_collection.utils.objects import Subject, Market, Bookmaker


def extract_teams_dict(data: dict) -> dict:
    # initialize a dictionary to hold team id and name
    teams_dict = dict()
    # for each dictionary in data's teams if they exist
    for team_data in data.get('teams', []):
        # get the team's id and abbreviated name, if both exist then keep going
        if (team_id := team_data.get('id')) and (team_abbr := team_data.get('abbr')):
            # store the team's id with its corresponding abbreviated name
            teams_dict[team_id] = team_abbr

    return teams_dict


def extract_game_and_player_dicts(data: dict) -> tuple[dict, dict]:
    # initialize dictionaries to hold corresponding ids
    game_ids_dict, player_ids_dict = dict(), dict()
    # for each dictionary in data's appearances if they exist
    for a_data in data.get('appearances', []):
        # get the general appearance id from the dictionary, if exists keep going
        if a_id := a_data.get('id'):
            # get the match id corresponding to a current game, if exists keep going
            if a_match_id := a_data.get('match_id'):
                # store the appearance id with the corresponding match id
                game_ids_dict[a_id] = a_match_id

            # get the player id from the dictionary, if exists keep going
            if a_player_id := a_data.get('player_id'):
                # store the appearance id with the corresponding player id
                player_ids_dict[a_id] = a_player_id

    # return both dictionaries
    return game_ids_dict, player_ids_dict


def extract_games_dict(data: dict) -> dict:
    # initialize a dictionary to hold game data
    games_dict = dict()
    # for each dictionary in data's games if they exist
    for game_data in data.get('games', []):
        # get the game id from the dictionary, if exists keep going
        if game_id := game_data.get('id'):
            # store the id along with the game data dictionary
            game_data[game_id] = game_data

    # return the game data dictionary
    return games_dict


def extract_solo_games_dict(data: dict) -> dict:
    # initialize dictionary to hold solo games data
    solo_games_dict = dict()
    # for each dictionary in data's solo games, if they exit
    for solo_game_data in data.get('solo_games', []):
        # get the game id from the dictionary, if it exists keep going
        if game_id := solo_game_data.get('id'):
            # store the game id corresponding to the dictionary of solo game data
            solo_game_data[game_id] = solo_game_data

    # return the dictionary of ids and data
    return solo_games_dict


def extract_subject_team(data: dict, teams_dict: dict) -> Optional[str]:
    # get the team id from the dictionary, if exists keep going
    if team_id := data.get('team_id'):
        # return the team name from the dictionary if it exists
        return teams_dict.get(team_id)


def extract_player_name(data: dict) -> Optional[str]:
    # get the player's first and last name, if either exist then keep executing
    if (first_name := data.get('first_name', '**')) and (last_name := data.get('last_name', '**')):
        # return the full player name (the stars are only to ease conditional logic, if one of the names is empty)
        return ' '.join([first_name.replace('**', ''), last_name.replace('**', '')])


def extract_players_dict(data: dict, teams_dict: dict) -> dict:
    # initialize a dictionary to hold player ids and data
    players_dict = dict()
    # for each dictionary in data's players if they exist
    for player_data in data.get('players', []):
        # get the player id from the dictionary, if it exists keep executing
        if player_id := player_data.get('id'):
            # store the id along with corresponding player data
            player_data[player_id] = {
                'subject': extract_player_name(player_data),
                'subject_team': extract_subject_team(player_data, teams_dict)
            }

    # return the players dict
    return players_dict


def extract_appearance(data: dict) -> Optional[tuple[str, dict]]:
    # get a dictionary that holds match ids, if exists keep executing
    if (over_under_data := data.get('over_under')) and (a_stat_data := over_under_data.get('appearance_stat')):
        # return the dictionary containing market and match data
        return a_stat_data.get('appearance_id'), a_stat_data


def extract_match_data(a_id: str, game_ids_dict: dict, games_dict: dict, solo_games_dict: dict) -> Optional[dict]:
    # get the appearance id and then game id, if both exist keep executing
    if g_id := game_ids_dict.get(a_id):
        # return the game data, if it doesn't exist check the solo games dictionary and return that
        return games_dict.get(g_id, solo_games_dict.get(g_id))


def extract_league(a_id: str, game_ids_dict: dict, games_dict: dict, solo_games_dict: dict) -> Optional[str]:
    # get the match data associated with some ids, if exists then keep executing
    if match_data := extract_match_data(a_id, game_ids_dict, games_dict, solo_games_dict):
        # get the league name, if exists then keep executing
        if league := match_data.get('sport_id'):
            # clean the league name
            cleaned_league = utils.clean_league(league)
            # check if the cleaned league name is valid
            if utils.is_league_valid(cleaned_league):
                # return the cleaned and valid league name
                return cleaned_league


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get market and market id
    if market := data.get('display_stat'):
        # get the cleaned market name
        cleaned_market = clean_market(market, league)
        # return the market id from the db and the clean market name
        return get_market_id(Market(cleaned_market, league)), cleaned_market


def extract_subject(a_id: str, league: str, player_ids_dict: dict, players_dict: dict) -> Optional[tuple[str, str]]:
    # get player id and dictionary, if both exist keep executing
    if (player_id := player_ids_dict.get(a_id)) and (player_data := players_dict.get(player_id)):
        # get the player name, if exists keep executing
        if subject := player_data.get('subject'):
            # clean the subject name
            cleaned_subject = clean_subject(subject)
            # return the subject id from the db and the cleaned subject name
            return get_subject_id(Subject(cleaned_subject, league, team=player_data.get('subject_team'))), cleaned_subject


def extract_label(data: dict) -> Optional[str]:
    # get the choice (label), if it exists then keep executing
    if choice := data.get('choice'):
        # Over equates to higher and Under equates to lower for label formatting
        return 'Over' if choice == 'higher' else 'Under'


def extract_multiplier(data: dict) -> Optional[float]:
    # get the multiplier for the prop line, if it exists keep executing
    if multiplier := data.get('payout_multiplier'):
        # return the multiplier cast from a str to a float
        return float(multiplier)


def get_odds(default_odds: float, multiplier: float) -> float:
    # return the product of the bookmaker's default odds stored in the db with the multiplier to get adjusted odds
    return round(default_odds * multiplier, 3)


class UnderdogFantasy(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url required to request teams data
        url = utils.get_url(name='teams')
        # get the headers required to request teams data
        headers = utils.get_headers(name='teams')
        # get the cookies required to get teams data
        cookies = utils.get_cookies(self.bookmaker_info.name)
        # make the request for teams data
        await self.req_mngr.get(url, self._parse_teams, headers=headers, cookies=cookies)

    async def _parse_teams(self, response) -> None:
        # get the response data as json, if exists keep going
        if json_data := response.json():
            # get the teams dict from response data, if exists keep going
            if teams_dict := extract_teams_dict(json_data):
                # get the required url to request for prop lines
                url = utils.get_url(self.bookmaker_info.name)
                # get the required headers to request for prop lines
                headers = utils.get_headers(self.bookmaker_info.name)
                # make the request for the prop lines
                await self.req_mngr.get(url, self._parse_lines, teams_dict, headers=headers)

    async def _parse_lines(self, response, teams_dict: dict) -> None:
        # get the response data as json, if exists keep going
        if json_data := response.json():
            # extract crucial id data from the dictionary
            game_ids_dict, player_ids_dict = extract_game_and_player_dicts(json_data)
            # extract game data into a dictionary from the response data
            games_dict = extract_games_dict(json_data)
            # extract the solo game data into a dictionary from the response data
            solo_games_dict = extract_solo_games_dict(json_data)
            # extract individual player data into a dictionary from response data
            players_dict = extract_players_dict(json_data, teams_dict)
            # for each dictionary in response data's over_under_lines if they exist
            for prop_line_data in json_data.get('over_under_lines', []):
                # get appearance statistical dictionary holding market and match data
                a_id, a_data = extract_appearance(prop_line_data)
                # if both exist then keep executing
                if a_id and a_data:
                    # extract the league from match data dictionary, if exists keep executing
                    if league := extract_league(a_id, game_ids_dict, games_dict, solo_games_dict):
                        # get the market id from db and the market name
                        market_id, market= extract_market(self.bookmaker_info.namea_data, league)
                        # if both exist then keep executing
                        if market_id and market:
                            # get the subject id from db and extract the subject name
                            subject_id, subject= extract_subject(self.bookmaker_info.namea_id, league, player_ids_dict, players_dict)
                            # if both exist keep executing
                            if subject_id and subject:
                                # get the numeric over/under line, if exists keep executing
                                if line := prop_line_data.get('stat_value'):
                                    # for each dictionary in prop_line_data's options if they exist
                                    for outcome_data in prop_line_data.get('options', []):
                                        # extract the multiplier from the dictionary, (might not exist but keep going)
                                        multiplier = extract_multiplier(outcome_data)
                                        # update shared data
                                        self.update_betting_lines({
                                            'batch_id': self.batch_id,
                                            'time_processed': datetime.now(),
                                            'league': league,
                                            'market_category': 'player_props',
                                            'market_id': str(market_id),
                                            'market': market_name,
                                            'subject_id': str(subject_id),
                                            'subject': subject_name,
                                            'bookmaker': self.bookmaker_info.name,
                                            'label': extract_label(outcome_data),
                                            'line': line,
                                            'multiplier': multiplier,
                                            'odds': get_odds(self.bookmaker_info.default_payout.odds, multiplier)
                                        })


if __name__ == "__main__":
    utils.run(UnderdogFantasy))
