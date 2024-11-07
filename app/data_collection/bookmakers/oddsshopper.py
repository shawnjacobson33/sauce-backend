from datetime import datetime, timedelta
import asyncio
from typing import Optional, Union, Any

from app import database as db
from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import utils as bkm_utils


BOOKMAKER_MAP = {
    'Underdog': 'Underdog Fantasy',
    'Pick6': 'DraftKingsPick6',
}


def extract_league(data: dict) -> Optional[str]:
    # get league and only execute if exists
    if league := data.get('leagueCode'):
        # clean the league name
        cleaned_league = bkm_utils.clean_league(league)
        # league must be valid
        if bkm_utils.is_league_valid(league):
            # return the valid and clean league name
            return cleaned_league


def get_dates() -> tuple[str, str]:
    # desired date_format
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    # Calculate and return current datetime and future date
    return datetime.now().strftime(date_format)[:-3] + 'Z', (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z'


def extract_market(bookmaker_name: str, data: dict, league: str) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get the market name, if it exists keep executing
    if market_name := data.get('offerName'):
        # create a market object
        market_obj = dc_utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = bkm_utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name

    return None, None


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get a list of dictionaries and the first dictionary in the list, if exists keep going
    if (participants_data := data.get('participants')) and (first_participants_data := participants_data[0]):
        # get the subject's name from the dictionary, if exists keep going
        if subject_name := first_participants_data.get('name'):
            # create a subject object
            subject_obj = dc_utils.Subject(subject_name, league)
            # gets the subject id or log message
            subject_id, subject_name = bkm_utils.get_subject_id(bookmaker_name, subject_obj)
            # return both subject id search result and cleaned subject
            return subject_id, subject_name

    return None, None


def extract_bookmaker(data: dict) -> Optional[str]:
    # get the bookmaker from the dictionary
    if bookmaker := data.get('sportsbookCode'):
        # return the bookmaker after checking if it needs formatting...to standardize across other plugs
        return BOOKMAKER_MAP.get(bookmaker, bookmaker)


def extract_true_win_probability(data: dict) -> Optional[float]:
    # gets the true win probability, if exists then execute
    if tw_prob := data.get('trueWinProbability'):
        # return a rounded true win probability by 3 decimal points
        return round(tw_prob, 3)


def extract_expected_value(data: dict) -> Optional[float]:
    # get the expected value, if it exists then execute
    if ev := data.get('ev'):
        # return the rounded expected value by 3 decimal points
        return round(ev, 3)


def extract_odds_and_other_stats(data: dict) -> Optional[tuple[float, float, float, float]]:
    # get the odds from dictionary, only execute if it exists
    if odds := data.get('odds'):
        # return the odds rounded by 3 decimal points, implied probability, true win probability, and expected value (calculated by OddsShopper)
        return round(odds, 3), round(1 / odds, 3), extract_true_win_probability(data), extract_expected_value(data)


class OddsShopper(bkm_utils.BookmakerPlug):
    def __init__(self, bookmaker_info: bkm_utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url to request matchups data
        url = bkm_utils.get_url(self.bookmaker_info.name, name='matchups')
        # get the headers to request matchups data
        headers = bkm_utils.get_headers(self.bookmaker_info.name)
        # get the cookies to request matchups data
        cookies = bkm_utils.get_cookies(self.bookmaker_info.name)
        # make request for matchups data
        await self.req_mngr.get(url, self._parse_matchups, headers=headers, cookies=cookies)

    async def _parse_matchups(self, response) -> None:
        # get json data from response and check its existence
        if json_data := response.json():
            # initialize tasks to hold all the requests to be made
            tasks = []
            # for each offer category dictionary in the response data's offerCategories if they exist
            for offer_category in json_data.get('offerCategories', []):
                # get the market category, only want data...FOR NOW... that is player props
                if offer_category.get('name') == 'PlayerProps':
                    # for each league dictionary of data in offer_category's offers if they exist
                    for league_data in offer_category.get('offers', []):
                        # extract the league from response data and offer id, only if both exist should it execute
                        if (league := extract_league(league_data)) and (offer_id := league_data.get('id')):
                            # get dates which are required for params of url for prop lines
                            start_date, end_date = get_dates()
                            # get the url required to request prop lines and format it with the offer id
                            url = bkm_utils.get_url(self.bookmaker_info.name).format(offer_id)
                            # get the headers required to request prop lines
                            headers = bkm_utils.get_headers(self.bookmaker_info.name)
                            # get the params required to request prop lines, using start_date, and end date
                            params = bkm_utils.get_params(self.bookmaker_info.name, var_1=start_date, var_2=end_date)
                            # add the request to tasks
                            tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=headers, params=params))

            # add all requests to the event loop and make the asynchronously
            await asyncio.gather(*tasks)

    # TODO: ANYWAY TO CHANGE game_info FORMAT? DON'T GET DATA FROM BOOKMAKERS YOU ALREADY HAVE?
    async def _parse_lines(self, response, league: str) -> None:
        # get json data from response and check its existence
        if json_data := response.json():
            # to track the leagues being collected
            self.metrics.add_league(league)
            # for each event/game in the response data
            for event in json_data:
                # gets the game info from the dictionary
                game_info = event.get('eventName')
                # get the market id from db and extract market from the data
                market_id, market_name = extract_market(self.bookmaker_info.name, event, league)
                # only keep executing if market id and market exist
                if market_id and market_name:
                    # to track the markets being collected
                    self.metrics.add_market((league, market_name))
                    # get the subject id from the db and extract the subject from outcome
                    subject_id, subject_name = extract_subject(self.bookmaker_info.name, event, league)
                    # only if both exist should execution keep going
                    if subject_id and subject_name:
                        # to track the subjects being collected
                        self.metrics.add_subject((league, subject_name))
                        # iterate through each side or bookmaker for the prop line
                        for side in event.get('sides', []):
                            # get the label from the side dictionary, only keep going if it exists
                            if label := side.get('label'):
                                # iterate through each outcome for each side
                                for outcome in side.get('outcomes', []):
                                    # get the bookmaker from outcome dictionary, if exists keep executing
                                    if bookmaker := extract_bookmaker(outcome):
                                        # extract odds, implied probability, true win probability, and expected value if they exist
                                        odds, impl_prob, tw_prob, ev = extract_odds_and_other_stats(outcome)
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
                                            'bookmaker': bookmaker,
                                            'label': label,
                                            'line': outcome.get('line', '0.5'),
                                            'odds': odds,
                                            'implied_prob': impl_prob,
                                            'other_stats': {
                                                'true_win_prob': tw_prob,
                                                'ev': ev
                                            }
                                        }, bookmaker=bookmaker)
