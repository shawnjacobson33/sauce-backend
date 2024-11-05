from datetime import datetime
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils


def extract_game_info(data: dict) -> Optional[str]:
    # get the game info and check if this isn't a Futures prop line, if so then keep executing
    if (game_info := data.get('gameInfo')) and ('Futures' not in game_info):
        # return the game info
        return game_info


def extract_league(data: dict) -> Optional[str]:
    # get the league name from dictionary, if it exists keep executing
    if league := data.get('league'):
        # clean the league name
        cleaned_league = utils.clean_league(league)
        # check if the league name is valid
        if utils.is_league_valid(cleaned_league):
            # return the cleaned and valid league name
            return cleaned_league


def extract_subject_team(data: dict) -> Optional[str]:
    # return the abbreviate team name, if it doesn't exist return the regular team name
    return data.get('abvTeamName', data.get('teamName'))


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the player's name, if it exists keep going
    if subject_name := data.get('name'):
        # get player attributes
        subject_team = extract_subject_team(data)
        # create a subject object
        subject_obj = utils.Subject(subject_name, league, team=subject_team)
        # gets the subject id or log message
        subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
        # return both subject id search result and cleaned subject
        return subject_id, subject_name

    return None, None


def extract_multiplier(data: dict, market: str) -> Optional[float]:
    # get some dictionaries that hold data about the multiplier, if both exist keep executing
    if (m_player_props_data := data.get('configPlayerProps')) and (m_market_data := m_player_props_data.get(market)):
        # return the multiplier cast from a str to float
        return float(m_market_data.get('multiplier', 1.0))


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market name from the dictionary, if exists keep going
    if market_name := data.get('p'):
        # create a market object
        market_obj = utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name

    return None, None


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

    # otherwise the default best odds available will just be used
    return default_odds

# TODO: DIFFERENT FORMAT FOR game_info?
class VividPicks(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url required to request for prop lines data
        url = utils.get_url(self.bookmaker_info.name)
        # get the headers required to request for prop lines data
        headers = utils.get_headers(self.bookmaker_info.name)
        # get the json required to request for prop lines data
        json_data = utils.get_json_data(self.bookmaker_info.name, )
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
                        # to track the leagues being collected
                        self.metrics.add_league(league)
                        # for each dictionary in event data's activePlayers if they exist
                        for player_data in event_data.get('activePlayers', []):
                            # get the subject id from db and extract the subject name from dictionary
                            subject_id, subject_name = extract_subject(self.bookmaker_info.name, player_data, league)
                            # if both exist keep executing
                            if subject_id and subject_name:
                                # to track the subjects being collected
                                self.metrics.add_subject((league, subject_name))
                                # for each dictionary in player data's visiblePlayerProps if they exist
                                for prop_line_data in player_data.get('visiblePlayerProps', []):
                                    # get the market id from the db and extract the market name from the dictionary
                                    market_id, market_name = extract_market(self.bookmaker_info.name, prop_line_data, league)
                                    # keep executing if both exist
                                    if market_id and market_name:
                                        # to track the markets being collected
                                        self.metrics.add_market((league, market_name))
                                        # get the numeric over/under line from the dictionary, keep going if exists
                                        if line := prop_line_data.get('val'):
                                            # extract the multiplier from the dictionary
                                            multiplier = extract_multiplier(player_data, market_name)
                                            # for each label (depending on the value of the multiplier)
                                            for label in get_labels(multiplier):
                                                # update shared data
                                                self.update_betting_lines({
                                                        'batch_id': self.batch_id,
                                                        'time_processed': str(datetime.now()),
                                                        'league': league,
                                                        'game_info': game_info,
                                                        'market_category': 'player_props',
                                                        'market_id': str(market_id),
                                                        'market': market_name,
                                                        'subject_id': str(subject_id),
                                                        'subject': subject_name,
                                                        'bookmaker': self.bookmaker_info.name,
                                                        'label': label,
                                                        'line': line,
                                                        'multiplier': multiplier,
                                                        'odds': get_odds(self.bookmaker_info.default_payout.odds, multiplier)
                                                    })


if __name__ == "__main__":
    utils.run(VividPicks)
