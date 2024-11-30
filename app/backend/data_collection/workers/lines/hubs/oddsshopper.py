from datetime import datetime, timedelta
from typing import Optional

from app.backend.data_collection.workers import utils as dc_utils
from app.backend.data_collection.workers.lines import utils as ln_utils


BOOKMAKER_MAP = {
    'Underdog': 'Underdog Fantasy',
    'Pick6': 'DraftKingsPick6',
}


def extract_league(data: dict) -> Optional[str]:
    # get league and only execute if exists
    if league := data.get('leagueCode'):
        # clean the league name
        cleaned_league = dc_utils.clean_league(league)
        # league must be valid
        if ln_utils.is_league_valid(league):
            # return the valid and clean league name
            return cleaned_league


def get_dates() -> tuple[str, str]:
    # desired date_format
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    # Calculate and return current datetime and future date
    return datetime.now().strftime(date_format)[:-3] + 'Z', (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z'


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the market name, if it exists keep executing
    if market_name := data.get('offerName'):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get a list of dictionaries and the first dictionary in the list, if exists keep going
    if (participants_data := data.get('participants')) and (first_participants_data := participants_data[0]):
        # get the subject's name from the dictionary, if exists keep going
        if subject_name := first_participants_data.get('name'):
            # return both subject id search result and cleaned subject
            return dc_utils.get_subject(bookmaker_name, league, subject_name)


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


class OddsShopper(ln_utils.LinesRetriever):
    def __init__(self, lines_hub: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(lines_hub)

    async def retrieve(self) -> None:
        # get the url to request matchups data
        url = ln_utils.get_url(self.name, name='matchups')
        # get the headers to request matchups data
        headers = ln_utils.get_headers(self.name)
        # get the cookies to request matchups data
        cookies = ln_utils.get_cookies(self.name)
        # make request for matchups data
        return await self.req_mngr.get(url, self._parse_matchups, headers=headers, cookies=cookies)

    async def _parse_matchups(self, response) -> list:
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
                            url = ln_utils.get_url(self.name).format(offer_id)
                            # get the headers required to request prop lines
                            headers = ln_utils.get_headers(self.name)
                            # get the params required to request prop lines, using start_date, and end date
                            params = ln_utils.get_params(self.name, var_1=start_date, var_2=end_date)
                            # add the request to tasks
                            tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=headers, params=params))

            # add all requests to the event loop and make the asynchronously
            return tasks

    # TODO: ANYWAY TO CHANGE game_info FORMAT? DON'T GET DATA FROM BOOKMAKERS YOU ALREADY HAVE?
    async def _parse_lines(self, response, league: str) -> None:
        # get json data from response and check its existence
        if json_data := response.json():
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # to track the leagues being collected
            dc_utils.RelevantData.update_relevant_leagues(league, self.name)
            # for each event/game in the response data
            for event in json_data:
                # get the market id from db and extract market from the data
                if market := extract_market(self.name, event, league):
                    # TODO: For Subjects Shared Data make sure to store a team id so that it can be used to get a game
                    # get the subject id from the db and extract the subject from outcome
                    if subject := extract_subject(self.name, event, league):
                        # use team data to get some game data
                        if game := dc_utils.get_game(league, subject['team']):
                            # iterate through each side or bookmaker for the prop line
                            for side in event.get('sides', []):
                                # get the label from the side dictionary, only keep going if it exists
                                if label := side.get('label'):
                                    # iterate through each outcome for each side
                                    for outcome in side.get('outcomes', []):
                                        # get the bookmaker from outcome dictionary, if exists keep executing
                                        if bookmaker_name := extract_bookmaker(outcome):
                                            # extract odds, implied probability, true win probability, and expected value if they exist
                                            odds, impl_prob, tw_prob, ev = extract_odds_and_other_stats(outcome)
                                            # update shared data
                                            dc_utils.BettingLines.update({
                                                's_tstamp': str(datetime.now()),
                                                'bookmaker': bookmaker_name,
                                                'sport': sport,
                                                'league': league,
                                                'game_time': game['game_time'],
                                                'game': game['info'],
                                                'market': market['name'],
                                                'subject_id': subject['id'],
                                                'subject': subject['name'],
                                                'label': label,
                                                'line': float(outcome.get('line', 0.5)),
                                                'odds': odds,
                                                'im_prb': impl_prob,
                                            })
