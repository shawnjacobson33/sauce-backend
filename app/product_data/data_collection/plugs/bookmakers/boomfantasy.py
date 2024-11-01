from datetime import datetime
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import helpers
from app.product_data.data_collection.utils import standardizing as std
from app.product_data.data_collection.plugs.bookmakers.reports import setup
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug
from app.product_data.data_collection.utils.objects import Subject, Market, Bookmaker


def extract_league(data: dict) -> Optional[str]:
    # get name of league, executes if exists
    if league := data.get('league'):
        # clean the league name
        cleaned_league = helpers.clean_league(league.upper())
        # checks if the league is valid
        if helpers.is_league_valid(cleaned_league):
            # cleans the league name
            return cleaned_league


def extract_game_info(data: dict) -> Optional[str]:
    # get a dictionary, if exists keep executing
    if game_filters_data := data.get('gameFilters'):
        # get the first dictionary
        game_filter_data = game_filters_data[0]
        # get a couple more dictionaries, if both exist keep executing
        if (game_title_data := game_filter_data.get('title')) and (teams_data := game_title_data.get('additionalOptions')):
            # get the away and home team names, if both exist keep executing
            if (away_team := teams_data.get('awayAbbreviation')) and (home_team := teams_data.get('homeAbbreviation')):
                # return game info
                return ' @ '.join([away_team, home_team])


def extract_player_info(data: dict) -> Optional[tuple[Optional[str], Optional[str]]]:
    # get a dictionary, if exists keep going
    if player_image_data := data.get('playerImage'):
        # get the abbreviated team name that the player is on and their jersey number
        return player_image_data.get('abbreviation'), player_image_data.get('jerseyNumber')


def extract_subject(data: dict, league: str) -> Optional[tuple[Optional[str], str]]:
    # gets the league section's title and options from that title, executes if they both exist
    if (title := data.get('title')) and (options := title.get('o')):
        # gets the first and last name of the player, executes if both exist
        if (first_name := options.get('firstName')) and (last_name := options.get('lastName')):
            # gets the full name of subject and cleans it
            cleaned_subject = helpers.clean_subject(' '.join([first_name, last_name]))
            # get some player attributes
            subject_team, jersey_number = extract_player_info(data)
            # gets the subject id of this player
            if subject_id := std.get_subject_id(Subject(cleaned_subject, league, team=subject_team, jersey_number=jersey_number)):
                # cast the subject id to a string
                subject_id = str(subject_id)

            # return both subject and subject id
            return subject_id, cleaned_subject


def extract_line(data: dict) -> Optional[tuple[str, str]]:
    # get the pick selection data, execute if exists
    if pick_selection_title := data.get('pickSelectionTitle'):
        # get additional data from pick selection, execute if exists
        if additional_options := pick_selection_title.get('additionalOptions'):
            # get market data and get numeric over/under line, execute if both exist
            if (stat_text := additional_options.get('statText')) and (line := additional_options.get('line')):
                # return the numeric line for the prop line and some additional market data
                return line, stat_text


def extract_period(data: dict) -> Optional[str]:
    # gets the period (fullGame, firstQuarter, etc.), keep executing if it exists and if it isn't fullGame
    if (period := data.get('periodClassifier')) and (period != 'fullGame'):
        # return the period classifier
        return period


def extract_market(data: dict, league: str, period: Optional[str] = None) -> Optional[tuple[Optional[str], str]]:
    # get the market name, if exists keep going
    if market := data.get("statistic"):
        # cleans the market
        cleaned_market = helpers.clean_market(market, 'boom_fantasy', period_classifier=period)
        # gets the market id
        if market_id := std.get_market_id(Market(cleaned_market, league)):
            # cast the market id to a string
            market_id = str(market_id)

        # return both market and market id
        return market_id, cleaned_market


def extract_label_and_odds(data: list) -> Optional[tuple[str, float]]:
    # check if the data list has the expected number of elements
    if len(data) == 3:
        # return a capitalized label and the odds
        return data[1].title(), float(data[2])


class BoomFantasy(BookmakerPlug):
    """
    BoomFantasy is a class that represents the process of collecting and parsing player prop lines from
    the BoomFantasy API. It inherits from the `Plug` class and utilizes asynchronous requests to gather
    data related to prop betting markets.

    Attributes:
        bookmaker_info (Bookmaker): An object containing information about the bookmaker.
        batch_id (str): A unique identifier for the batch of data being processed.
        req_mngr (RequestManager): Manages network requests, including handling authentication tokens.

    Methods:
        start(): Asynchronously starts the data collection process by sending a POST request to the
            BoomFantasy API to retrieve prop lines using authentication tokens.

        _parse_lines(response): Parses the API response to extract key information such as the league,
            player subjects, markets, and prop lines. This information is formatted and added to shared
            data storage for further use.
    """

    def __init__(self, bookmaker_info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id, req_mngr)

    async def start(self) -> None:
        # gets the url to get prop lines
        url = self.req_packager.get_url()
        # get the headers that will be sent with request for prop lines
        headers = self.req_packager.get_headers()
        # gets params that will be sent with request for prop lines
        params = self.req_packager.get_params()
        # gets valid tokens needed to access data
        tokens_data = {
            'url': self.req_packager.get_url(name='tokens'),
            'headers': self.req_packager.get_headers(name='tokens'),
            'json_data': self.req_packager.get_json_data(name='tokens')
        }
        # because of tokens, use a special get method to request data
        await self.req_mngr.get_bf(url, tokens_data, self._parse_lines, headers=headers, params=params)

    async def _parse_lines(self, response) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # get the contest from the data, executes if exists
            if contest := data.get('multiLineContest'):
                # for each section in the contest's sections if they exist
                for section_data in contest.get('sections', []):
                    # check if this section is active, if so keep executing
                    if section_data.get('status') == 'active':
                        # if they exist execute
                        if league := extract_league(section_data):
                            # extract the game info from the dictionary
                            game_info = extract_game_info(section_data)
                            # for each section in the league's sections if they exist
                            for qg_data in section_data.get('qG', []):
                                # extract the subject and get the subject id from the response data and database
                                subject_id, subject = extract_subject(qg_data, league)
                                # if they both exist then execute
                                if subject_id and subject:
                                    # get the period classifier from dictionary (fullGame, firstQuarter, etc.)
                                    period = extract_period(qg_data)
                                    # get more prop line info from the league's section's fullQuestions if they exist
                                    for q_data in qg_data.get('q', []):
                                        # extract the market and market id from the response data and database
                                        market_id, market = extract_market(q_data, league, period)
                                        # # if they both exist then execute
                                        # if market_id and market:
                                        # for each dictionary in q_data's c field
                                        for c_data in q_data.get('c', []):
                                            # extract the numeric line for the prop line, if exists keep going
                                            if line := c_data.get('l'):
                                                # for each over or under side to the prop line if they exist.
                                                for more_c_data in c_data.get('c', []):
                                                    # extract the label and multiplier from the list
                                                    label, odds = extract_label_and_odds(more_c_data)
                                                    # calculate the implied probability
                                                    implied_prob = 1 / odds
                                                    # if both exist the keep going
                                                    if label and odds:
                                                        # update shared data
                                                        self.update_betting_lines({
                                                            'batch_id': self.batch_id,
                                                            'time_processed': str(datetime.now()),
                                                            'league': league,
                                                            'game_info': game_info,
                                                            'market_category': 'player_props',
                                                            'market_id': market_id,
                                                            'market': market,
                                                            'subject_id': subject_id,
                                                            'subject': subject,
                                                            'bookmaker': self.bookmaker_info.name,
                                                            'label': label,
                                                            'line': line,
                                                            'odds': odds,
                                                            'implied_prob': implied_prob
                                                        })



if __name__ == "__main__":
    setup.run(BoomFantasy)
