import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils
from app.product_data.data_collection.utils import standardizing as std
from app.product_data.data_collection.plugs.bookmakers.utils import setup
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug
from app.product_data.data_collection.utils.objects import Subject, Market, Bookmaker


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
            if is_league_valid(league):
                # return the cleaned league name
                return clean_league(league)


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get 3 different dictionaries holding market data, if all exist keep going
    if (choices := data.get('choices')) and (actor := choices[0].get('actor')) and (requirements := actor.get('winningRequirement')):
        # get the market name from the dictionary, if exists keep going
        if market := requirements[0].get('name'):
            # clean the market name
            cleaned_market = clean_market(market, league)
            # return the market id and cleaned market name
            return get_market_id(Market(cleaned_market, league)), cleaned_market


def extract_position(data: dict) -> Optional[str]:
    # get the abbreviated player's position, if exists keep going
    if position := data.get('posAbbr'):
        # return the cleaned player's position
        return clean_position(position)


def extract_subject(data: dict, league: str) -> Optional[tuple[Union[ObjectId, str], str]]:
    # get player data dictionary, if exists keep going
    if player_data := data.get('player'):
        # get the first and last name of the player, if both exist keep going
        if (first_name := player_data.get('fName')) and (first_name != 'combined') and (last_name := player_data.get('lName')):
            # get the player's full name and clean it
            cleaned_subject = clean_subject(' '.join([first_name, last_name]))
            # return the subject's id from the db and the cleaned subject name and some game info
            return get_subject_id(Subject(cleaned_subject, league, player_data.get('teamAbbr'), extract_position(player_data))), cleaned_subject, player_data.get('eventName')


class SuperDraft(BookmakerPlug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def collect(self) -> None:
        # get the url required to request prop lines data
        url = utils.get_url()
        # get the headers required to request prop lines data
        headers = utils.get_headers()
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
                    # get the market id and extract the market name from the dictionary
                    market_id, market, message = extract_market(prop_line_data, league)
                    # if both exist then keep going
                    if market_id and market:
                        # get the subject id from the db and extract the subject name from the dictionary
                        subject_id, subject, game_info = extract_subject(prop_line_data, league)
                        # if both exist then keep going
                        if subject_id and subject:
                            # get the numeric over/under line from the dictionary
                            if line := prop_line_data.get('line'):
                                # for each generic over/under label for prop lines
                                for label in ['Over', 'Under']:
                                    # update shared data
                                    self.add_and_update({
                                        'batch_id': self.batch_id,
                                        'time_processed': datetime.now(),
                                        'league': league,
                                        'market_category': 'player_props',
                                        'market_id': market_id,
                                        'market': market,
                                        'game_info': game_info,
                                        'subject_id': subject_id,
                                        'subject': subject,
                                        'bookmaker': self.info.name,
                                        'label': label,
                                        'line': line,
                                        'odds': self.info.default_payout.odds
                                    })


if __name__ == "__main__":
    asyncio.run(run(SuperDraft))
