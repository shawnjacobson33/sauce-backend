import asyncio
from datetime import datetime
from collections import defaultdict
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league, clean_position


def extract_league(data: dict) -> Optional[str]:
    # get the league from the data, if exists keep executing
    if league := data.get('sport'):
        # clean the league name
        cleaned_league = clean_league(league)
        # check if the league name is valid
        if is_league_valid(cleaned_league):
            # return the clean league name
            return cleaned_league


def extract_position(data: dict) -> Optional[str]:
    # get the player's position from the dictionary, if exists keep going
    if position := data.get('position'):
        # return the clean player position
        return clean_position(position)


def extract_subject(data: dict, players_dict: dict, league: str) -> Optional[tuple[str, str]]:
    # get the player's id and their data from dictionaries, if both exist keep executing
    if (player_id := data.get('subject_id')) and (player_data := players_dict.get(league).get(player_id)):
        # get the subject's name from the dictionary
        if subject := player_data.get('subject'):
            # get the cleaned subject name
            cleaned_subject = clean_subject(subject)
            # return the subject id from the db and the cleaned subject name
            return get_subject_id(Subject(cleaned_subject, league, player_data.get('subject_team'), extract_position(player_data))), cleaned_subject


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market name from the dictionary, if exists keep going
    if market := data.get('wager_type'):
        # get the cleaned market name
        cleaned_market = clean_market(market, league)
        # return the market id from db and the cleaned market name
        return get_market_id(Market(market, league)), cleaned_market


def extract_label(data: dict) -> Optional[str]:
    # get the label from the dictionary
    if label := data.get('outcome'):
        # return the label capitalized
        return label.title()

# TODO: Sleeper recently added alt_lines
class Sleeper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)
        # get universally used headers to make requests
        self.headers = self.req_packager.get_headers()

    async def start(self) -> None:
        # get the url required to request player data
        url = self.req_packager.get_url(name='players')
        # get params required to request player data
        params = self.req_packager.get_params()
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
                if subject_team := player_data.get('team') and (player_id := player_data.get('player_id')):
                    # get subject's first and last name, if both exist then keep executing
                    if (first_name := player_data.get('first_name')) and (last_name := player_data.get('last_name')):
                        # get the subject's sport, if exists then keep executing
                        if subject_sport := player_data.get('sport'):
                            # store data about the subject, corresponding to their sport and id
                            players_dict[subject_sport][player_id] = {
                                'subject_team': subject_team,
                                'subject': ' '.join([first_name, last_name]),
                                'position': player_data.get('position')
                            }

            # get the url required to make request for prop lines
            url = self.req_packager.get_url()
            # make the request for prop lines data
            await self.req_mngr.get(url, self._parse_lines, players_dict, headers=self.headers)

    async def _parse_lines(self, response, players: dict) -> None:
        # get response data as json, if exists keep going
        if json_data := response.json():
            # for each dictionary of data in response
            for prop_line_data in json_data:
                # extract the league name from dictionary
                if league := extract_league(prop_line_data):
                    # get the subject id from db and extract player name from dictionary
                    subject_id, subject = extract_subject(prop_line_data, players, league)
                    # if both exist then keep going
                    if subject_id and subject:
                        # get the market id from db and extract the market name from dictionary
                        market_id, market = extract_market(prop_line_data, league)
                        # if both exist then keep executing
                        if market_id and market:
                            # for each dictionary containing label, line, odds in prop_line_data's options if exists
                            for outcome_data in prop_line_data.get('options', []):
                                # get the numeric over/under line and the decimal odds from the dictionary, if both exist keep going
                                if (line := outcome_data.get('outcome_value')) and (odds := prop_line_data.get('payout_multiplier')):
                                    # get the over or under label from the dictionary, if exists keep going
                                    if label := extract_label(outcome_data):
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
                                            'odds': odds
                                        })


if __name__ == "__main__":
    asyncio.run(run(Sleeper))
