from datetime import datetime
from typing import Optional, Union, Any

from app.data_collection.bookmakers import utils as bkm_utils


def extract_league(data: dict) -> Optional[str]:
    # get the league from data, if exists then keep going
    if league := data.get('league'):
        # clean the league
        cleaned_league = bkm_utils.clean_league(league)
        # check if league is valid
        if bkm_utils.is_league_valid(league):
            # return valid and clean league
            return cleaned_league


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the market from data, if exists keep going
    if market_data := data.get('bet_text'):
        # get the market name
        market_name = market_data.split(' (')[0]
        # gets the market id or log message
        market = bkm_utils.get_market_id(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_team(bookmaker_name: str, league: str, data: list) -> Optional[dict[str, str]]:
    # extract the subject team
    subject_team = data[-1][1:-1].replace('r.(', '')
    # get the team id and team name from the database
    if team_data := bkm_utils.get_team_id(bookmaker_name, league, subject_team):
        # return the team id and team name
        return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the subject data from data, if exists keep going
    if subject_data := data.get('title'):
        # splits the data into sub components containing individual attributes
        subject_components = subject_data.split()
        # get subject name
        subject_name = ' '.join(subject_components[:-1])
        # get the player's team
        team = extract_team(bookmaker_name, league, subject_components)
        # gets the subject id or log message
        subject = bkm_utils.get_subject_id(bookmaker_name, league, subject_name, team=team)
        # return both subject id search result and cleaned subject
        return subject


def extract_line_and_label(data: dict) -> Union[tuple[Any, Any], tuple[None, None]]:
    # there are 2 option components 1, 2
    for i in range(2):
        # get the data that holds the line and label
        if line_and_label_data := data.get(f'option_{i+1}'):
            # get the individual components as a list
            line_and_label_components = line_and_label_data.split()
            # verify that there are two components, line and label
            if len(line_and_label_components) == 2:
                # yield the line and label
                yield line_and_label_components[1], line_and_label_components[0].lower().title()


# TODO: CHECK IS_BOOSTED LOGIC
class MoneyLine(bkm_utils.BookmakerPlug):
    def __init__(self, bookmaker_info: bkm_utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # gets the url to get prop lines
        url = bkm_utils.get_url(self.bookmaker_info.name)
        # gets the headers to make request for prop lines
        headers = bkm_utils.get_headers(self.bookmaker_info.name)
        # gets the cookies to make request for prop lines
        cookies = bkm_utils.get_cookies(self.bookmaker_info.name)
        # gets the params to make request for prop lines
        params = bkm_utils.get_params(self.bookmaker_info.name)
        # makes request for prop lines
        await self.req_mngr.get(url, self._parse_lines, headers=headers, cookies=cookies, params=params)

    async def _parse_lines(self, response):
        # gets the json data from the response and then the data from bets field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('bets', [])):
            # for each prop line in the data, if they exist
            for prop_line in data:
                # extract the league name, keep going if it exists
                if league := extract_league(prop_line):  # TODO: BUG - GETTING "NFL" AS LEAGUE FOR NCAAF PLAYERS
                    # to track the leagues being collected
                    bkm_utils.Leagues.update_valid_leagues(self.bookmaker_info.name, league)
                    # extract the market id from database and market name from dictionary
                    if market := extract_market(self.bookmaker_info.name, prop_line, league):
                        # extract the subject id and subject name from the database and dictionary respectively
                        if subject := extract_subject(self.bookmaker_info.name, prop_line, league):
                            # get line and label for every one that exists
                            for line, label in extract_line_and_label(prop_line):
                                # update shared data
                                self.update_betting_lines({
                                    'batch_id': self.batch_id,
                                    'time_processed': str(datetime.now()),
                                    'league': league,
                                    'market_category': 'player_props',
                                    'market_id': market['id'],
                                    'market': market['name'],
                                    'subject_id': subject['id'],
                                    'subject': subject['name'],
                                    'bookmaker': self.bookmaker_info.name,
                                    'label': label,
                                    'line': line,
                                    'odds': self.bookmaker_info.default_payout.odds,
                                    'is_boosted': 'Discount' in market['name'] # TODO: COULD BE A BUG HERE...DEFINITELY NOT GOING TO WORK
                                })
