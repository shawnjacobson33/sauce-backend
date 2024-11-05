from datetime import datetime
from typing import Optional, Union

from bson import ObjectId

from app.product_data.data_collection.plugs.bookmakers import utils


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
            if utils.is_league_valid(league):
                # return the cleaned league name
                return utils.clean_league(league)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get 3 different dictionaries holding market data, if all exist keep going
    if (choices := data.get('choices')) and (actor := choices[0].get('actor')):
        if requirements := actor.get('winningRequirement'):
            # get the market name from the dictionary, if exists keep going
            if market_name := requirements[0].get('name'):
                # create a market object
                market_obj = utils.Market(market_name, league=league)
                # gets the market id or log message
                market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
                # return both market id search result and cleaned market
                return market_id, market_name

    return None, None


def extract_position(data: dict) -> Optional[str]:
    # get the abbreviated player's position, if exists keep going
    if position := data.get('posAbbr'):
        # return the cleaned player's position
        return utils.clean_position(position)


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str, Optional[str]]]:
    # get player data dictionary, if exists keep going
    if player_data := data.get('player'):
        # get the first and last name of the player, if both exist keep going
        if (first_name := player_data.get('fName')) and (first_name != 'combined') and (last_name := player_data.get('lName')):
            # get some game info
            game_info = player_data.get('eventName')
            # get player attributes
            subject_team, position = player_data.get('teamAbbr'), extract_position(player_data)
            # create a subject object
            subject_obj = utils.Subject(' '.join([first_name, last_name]), league, subject_team, position)
            # gets the subject id or log message
            subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
            # return both subject id search result and cleaned subject
            return subject_id, subject_name, game_info

    return None, None


class SuperDraft(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url required to request prop lines data
        url = utils.get_url(self.bookmaker_info.name)
        # get the headers required to request prop lines data
        headers = utils.get_headers(self.bookmaker_info.name)
        # make the request for the prop lines
        await self.req_mngr.get(url, self._parse_lines, headers=headers)

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
                    self.metrics.add_league(league)
                    # get the market id and extract the market name from the dictionary
                    market_id, market_name = extract_market(self.bookmaker_info.name, prop_line_data, league)
                    # if both exist then keep going
                    if market_id and market_name:
                        # to track the markets being collected
                        self.metrics.add_market((league, market_name))
                        # get the subject id from the db and extract the subject name from the dictionary
                        subject_id, subject_name, game_info = extract_subject(self.bookmaker_info.name, prop_line_data, league)
                        # if both exist then keep going
                        if subject_id and subject_name:
                            # to track the subjects being collected
                            self.metrics.add_subject((league, subject_name))
                            # get the numeric over/under line from the dictionary
                            if line := prop_line_data.get('line'):
                                # for each generic over/under label for prop lines
                                for label in ['Over', 'Under']:
                                    # update shared data
                                    self.update_betting_lines({
                                        'batch_id': self.batch_id,
                                        'time_processed': str(datetime.now()),
                                        'league': league,
                                        'market_category': 'player_props',
                                        'market_id': str(market_id),
                                        'market': market_name,
                                        'game_info': game_info,
                                        'subject_id': str(subject_id),
                                        'subject': subject_name,
                                        'bookmaker': self.bookmaker_info.name,
                                        'label': label,
                                        'line': line,
                                        'odds': self.bookmaker_info.default_payout.odds
                                    })


if __name__ == "__main__":
    utils.run(SuperDraft)
