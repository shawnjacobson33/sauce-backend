import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league


def extract_game_info(data: dict) -> Optional[str]:
    # get the game info and check if this isn't a Futures prop line, if so then keep executing
    if (game_info := data.get('gameInfo')) and ('Futures' not in game_info):
        # return the game info
        return game_info


def extract_league(data: dict) -> Optional[str]:
    # get the league name from dictionary, if it exists keep executing
    if league := data.get('league'):
        # clean the league name
        cleaned_league = clean_league(league)
        # check if the league name is valid
        if is_league_valid(cleaned_league):
            # return the cleaned and valid league name
            return cleaned_league


def extract_subject_team(data: dict) -> Optional[str]:
    # return the abbreviate team name, if it doesn't exist return the regular team name
    return data.get('abvTeamName', data.get('teamName'))


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the player's name, if it exists keep going
    if subject := data.get('name'):
        # clean the player's name
        cleaned_subject = clean_subject(subject)
        # return the subject id from the db and the cleaned subject name
        return get_subject_id(Subject(cleaned_subject, league, extract_subject_team(data))), cleaned_subject


def extract_multiplier(data: dict, market: str) -> Optional[float]:
    # get some dictionaries that hold data about the multiplier, if both exist keep executing
    if (m_player_props_data := data.get('configPlayerProps')) and (m_market_data := m_player_props_data.get(market)):
        # return the multiplier cast from a str to float
        return float(m_market_data.get('multiplier', 1.0))


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market name from the dictionary, if exists keep going
    if market := data.get('p'):
        # clean the market name
        cleaned_market = clean_market(market)
        # return the market id from db and the cleaned market name
        return get_market_id(Market(market, league)), cleaned_market


def get_labels(multiplier: float) -> list:
    # generic labels for prop line
    labels = ['Over', 'Under']
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


def get_odds(default_odds: float, multiplier: float) -> float:
    # return the product of the bookmaker's default odds stored in the db with the multiplier to get adjusted odds
    return round(default_odds * multiplier, 3)


class VividPicks(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def start(self) -> None:
        # get the url required to request for prop lines data
        url = self.req_packager.get_url()
        # get the headers required to request for prop lines data
        headers = self.req_packager.get_headers()
        # get the json required to request for prop lines data
        json_data = self.req_packager.get_json_data()
        # make the request for prop lines data
        await self.req_mngr.post(url, self._parse_lines, headers=headers, json=json_data)

    async def _parse_lines(self, response) -> None:
        # get the response data as json, execute if it exists
        if json_data := response.json():
            # for each dictionary in the response data's gret if it exists
            for event_data in json_data.get('gret', []):
                # extract the game info from the dictionary, if it exists keep going
                if game_info := extract_game_info(event_data):
                    # extract the league name from dictionary, if it exists keep going
                    if league := extract_league(event_data):
                        # for each dictionary in event data's activePlayers if they exist
                        for player_data in event_data.get('activePlayers', []):
                            # get the subject id from db and extract the subject name from dictionary
                            subject_id, subject = extract_subject(player_data, league)
                            # if both exist keep executing
                            if subject_id and subject:
                                # for each dictionary in player data's visiblePlayerProps if they exist
                                for prop_line_data in player_data.get('visiblePlayerProps', []):
                                    # get the market id from the db and extract the market name from the dictionary
                                    market_id, market = extract_market(prop_line_data, league)
                                    # keep executing if both exist
                                    if market_id and market:
                                        # get the numeric over/under line from the dictionary, keep going if exists
                                        if line := prop_line_data.get('val'):
                                            # extract the multiplier from the dictionary
                                            multiplier = extract_multiplier(player_data, market)
                                            # for each label (depending on the value of the multiplier)
                                            for label in get_labels(multiplier):
                                                # update shared data
                                                self.add_and_update({
                                                        'batch_id': self.batch_id,
                                                        'time_processed': datetime.now(),
                                                        'league': league,
                                                        'game_info': game_info,
                                                        'market_category': 'player_props',
                                                        'market_id': market_id,
                                                        'market': market,
                                                        'subject_id': subject_id,
                                                        'subject': subject,
                                                        'bookmaker': self.info.name,
                                                        'label': label,
                                                        'line': line,
                                                        'multiplier': multiplier,
                                                        'odds': get_odds(self.info.default_payout.odds, multiplier)
                                                    })


if __name__ == "__main__":
    asyncio.run(run(VividPicks))
