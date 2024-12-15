from typing import Optional

from app import utils as dc_utils
from app import utils as ln_utils


def extract_league(name: str, data: dict) -> Optional[str]:
    # get name of league, executes if exists
    if league := data.get('league'):
        # clean the league name
        cleaned_league = dc_utils.clean_league(league.upper())
        # checks if the league is valid
        if ln_utils.is_league_valid(cleaned_league):
            # to track the leagues being collected
            dc_utils.RelevantData.update_relevant_leagues(cleaned_league, name)
            # cleans the league name
            return cleaned_league


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[tuple[str, str]]:
    # get a dictionary, if exists keep going
    if player_image_data := data.get('playerImage'):
        # get the team name if it exists
        if team_name := player_image_data.get('abbreviation'):
            # get the team id and team name from the database
            return dc_utils.get_team(bookmaker_name, league, team_name)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: str) -> Optional[dict[str, str]]:
    # gets the league section's title and options from that title, executes if they both exist
    if (title := data.get('title')) and (options := title.get('o')):
        # gets the first and last name of the player, executes if both exist
        if (first_name := options.get('firstName')) and (last_name := options.get('lastName')):
            # get subject name
            subject_name = ' '.join([first_name, last_name])
            # DATA LOOKS LIKE --> {'id': 123asd, 'name': 'Jayson Tatum'} POSSIBLY WITH 'team': 'BOS'
            return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


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


def extract_market(bookmaker_name: str, data: dict, league: str, period_type: Optional[str] = None) -> Optional[dict[str, str]]:
    # get the market name, if exists keep going
    if market_name := data.get("statistic"):
        # gets the market id
        market = dc_utils.get_market(bookmaker_name, league, market_name, period_type=period_type)
        # return both market id and cleaned market
        return market


def extract_label_and_odds(data: list) -> Optional[tuple[str, float]]:
    # check if the data list has the expected number of elements
    if len(data) == 3:
        # return a capitalized label and the odds
        return data[1].title(), float(data[2])


class BoomFantasy(ln_utils.LinesRetriever):
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

    def __init__(self, batch_id: str, bookmaker: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(batch_id, bookmaker)

    async def retrieve(self) -> None:
        # gets the url to get prop lines
        url = ln_utils.get_url(self.name)
        # get the headers that will be sent with request for prop lines
        headers = ln_utils.get_headers(self.name)
        # gets params that will be sent with request for prop lines
        params = ln_utils.get_params(self.name)
        # gets valid tokens needed to access data
        tokens_data = {
            'url': ln_utils.get_url(self.name, name='tokens'),
            'headers': ln_utils.get_headers(self.name, name='tokens'),
            'json_data': ln_utils.get_json_data(self.name, name='tokens')
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
                        if league := extract_league(self.name, section_data):
                            # get the sport for this league
                            sport = dc_utils.LEAGUE_SPORT_MAP[league]
                            # for each section in the league's sections if they exist
                            for qg_data in section_data.get('qG', []):
                                # DATA LOOKS LIKE --> ('NBA', 'BOS')
                                if team_id := extract_team(self.name, league, qg_data):
                                    # DATA LOOKS LIKE --> {'info': 'BOS @ BKN','box_score_url': 'NBA_20241113_BOS@BKN','game_time': 2024-12-03 20:00:00}
                                    if game := dc_utils.get_game(team_id):
                                        # DATA LOOKS LIKE --> {'id': 123asd, 'name': 'Jayson Tatum'} POSSIBLY WITH 'team': 'BOS'
                                        if subject := extract_subject(self.name, qg_data, league, team_id[1]):
                                            # get the period classifier from dictionary (fullGame, firstQuarter, etc.)
                                            period = extract_period(qg_data)
                                            # get more prop line info from the league's section's fullQuestions if they exist
                                            for q_data in qg_data.get('q', []):
                                                # extract the market and market id from the response data and database
                                                if market := extract_market(self.name, q_data, league, period):
                                                    # for each dictionary in q_data's c field
                                                    for c_data in q_data.get('c', []):
                                                        # extract the numeric line for the prop line, if exists keep going
                                                        if line := c_data.get('l'):
                                                            # for each over or under side to the prop line if they exist.
                                                            for more_c_data in c_data.get('c', []):
                                                                # extract the label and multiplier from the list
                                                                label, odds = extract_label_and_odds(more_c_data)
                                                                # if both exist the keep going
                                                                if label and odds:
                                                                    # update shared data
                                                                    self.store({
                                                                        'bookmaker': self.name,
                                                                        'sport': sport,
                                                                        'league': league,
                                                                        'game_time': game['game_time'],
                                                                        'game': game['info'],
                                                                        'market': market['name'],
                                                                        'subject_id': subject['id'],
                                                                        'subject': subject['name'],
                                                                        'label': label,
                                                                        'line': line,
                                                                        'odds': odds,
                                                                        'im_prb': round(1 / odds, 4)
                                                                    })
