from datetime import datetime
from collections import defaultdict
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils


def extract_league(data: dict) -> Optional[str]:
    # get the league from the data, if exists keep executing
    if league := data.get('sport'):
        # clean the league name
        cleaned_league = utils.clean_league(league)
        # check if the league name is valid
        if utils.is_league_valid(cleaned_league):
            # return the clean league name
            return cleaned_league


def extract_position(data: dict) -> Optional[str]:
    # get the player's position from the dictionary, if exists keep going
    if position := data.get('position'):
        # return the clean player position
        return utils.clean_position(position)


def extract_subject(bookmaker_name: str, data: dict, players_dict: dict, league: str) -> Optional[tuple[str, str]]:
    # get the player's id and their data from dictionaries, if both exist keep executing
    if (player_id := data.get('subject_id')) and (league_data := players_dict.get(league)):
        # get some player data dictionary
        if player_data := league_data.get(player_id):
            # get the subject's name from the dictionary
            if subject_name := player_data.get('subject'):
                # get player attributes
                subject_team, position = player_data.get('subject_team'), extract_position(player_data)
                # create a subject object
                subject_obj = utils.Subject(subject_name, league, team=subject_team, position=position)
                # gets the subject id or log message
                subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
                # return both subject id search result and cleaned subject
                return subject_id, subject_name

    return None, None


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market name from the dictionary, if exists keep going
    if market_name := data.get('wager_type'):
        # create a market object
        market_obj = utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name

    return None, None


def extract_label(data: dict) -> Optional[str]:
    # get the label from the dictionary
    if label := data.get('outcome'):
        # return the label capitalized
        return label.title()

# TODO: Sleeper recently added alt_lines
class Sleeper(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)
        # get universally used headers to make requests
        self.headers = utils.get_headers(self.bookmaker_info.name)

    async def collect(self) -> None:
        # get the url required to request player data
        url = utils.get_url(self.bookmaker_info.name, name='players')
        # get params required to request player data
        params = utils.get_params(self.bookmaker_info.name)
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
                            cleaned_league = utils.clean_league(subject_sport)
                            if utils.is_league_valid(cleaned_league):
                                # store data about the subject, corresponding to their sport and id
                                players_dict[cleaned_league][player_id] = {
                                    'subject_team': subject_team,
                                    'subject': ' '.join([first_name, last_name]),
                                    'position': player_data.get('position')
                                }

            # get the url required to make request for prop lines
            url = utils.get_url(self.bookmaker_info.name)
            # make the request for prop lines data
            await self.req_mngr.get(url, self._parse_lines, players_dict, headers=self.headers)

    async def _parse_lines(self, response, players: dict) -> None:
        # get response data as json, if exists keep going
        if json_data := response.json():
            # for each dictionary of data in response
            for prop_line_data in json_data:
                # extract the league name from dictionary
                if league := extract_league(prop_line_data):
                    # to track the leagues being collected
                    self.metrics.add_league(league)
                    # get the subject id from db and extract player name from dictionary
                    subject_id, subject_name = extract_subject(self.bookmaker_info.name, prop_line_data, players, league)
                    # if both exist then keep going
                    if subject_id and subject_name:
                        # to track the subjects being collected
                        self.metrics.add_subject((league, subject_name))
                        # get the market id from db and extract the market name from dictionary
                        market_id, market_name = extract_market(self.bookmaker_info.name, prop_line_data, league)
                        # if both exist then keep executing
                        if market_id and market_name:
                            # to track the markets being collected
                            self.metrics.add_market((league, market_name))
                            # for each dictionary containing label, line, odds in prop_line_data's options if exists
                            for outcome_data in prop_line_data.get('options', []):
                                # get the numeric over/under line and the decimal odds from the dictionary, if both exist keep going
                                if (line := outcome_data.get('outcome_value')) and (odds := outcome_data.get('payout_multiplier')):
                                    # get the over or under label from the dictionary, if exists keep going
                                    if label := extract_label(outcome_data):
                                        # update shared data
                                        self.update_betting_lines({
                                            'batch_id': self.batch_id,
                                            'time_processed': str(datetime.now()),
                                            'league': league,
                                            'market_category': 'player_props',
                                            'market_id': str(market_id),
                                            'market': market_name,
                                            'subject_id': str(subject_id),
                                            'subject': subject_name,
                                            'bookmaker': self.bookmaker_info.name,
                                            'label': label,
                                            'line': line,
                                            'odds': float(odds)
                                        })


if __name__ == "__main__":
    utils.run(Sleeper)
