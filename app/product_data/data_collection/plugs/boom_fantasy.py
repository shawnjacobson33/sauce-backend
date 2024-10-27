import asyncio
import os
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league


def read_tokens():
    # get the working directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # get the absolute file path where the tokens are stored
    absolute_path = os.path.join(script_dir, 'tokens', 'boomfantasy_tokens.txt')
    # open the file in read mode
    with open(absolute_path, 'r') as file:
        # extract the access token and refresh token
        access_token, refresh_token = [line.strip() for line in file.readlines()[:2]]

    # return the extracted data in a dictionary
    return {'path': absolute_path, 'access_token': access_token, 'refresh_token': refresh_token}


def extract_league(section: dict) -> Optional[tuple[dict, str]]:
    # don't need 'matchups' data...only concerned about individual subject prop lines
    if section.get('type') == 'overUnder':
        # for each league in the section's leagues if they exist
        for league_data in section.get('leagues', []):
            # get name of league, executes if exists
            if league := league_data.get('league'):
                # cleans the league name
                return league_data, clean_league(league.upper())


def extract_subject(league_section: dict, league: str) -> Optional[tuple[Optional[str], str]]:
    # get the abbreviated team name that the player is on from the player's image if it exists
    subject_team = player_image.get('abbreviation') if (player_image := league_section.get('playerImage')) else None
    # gets the league section's title and options from that title, executes if they both exist
    if (title := league_section.get('title')) and (options := title.get('additionalOptions')):
        # gets the first and last name of the player, executes if both exist
        if (first_name := options.get('firstName')) and (last_name := options.get('lastName')):
            # gets the full name of subject and cleans it
            subject = clean_subject(' '.join([first_name, last_name]))
            # gets the subject id of this player
            subject_id = get_subject_id(Subject(subject, league, subject_team))
            # return both subject and subject id
            return subject_id, subject


def extract_line(data: dict) -> Optional[tuple[str, str]]:
    # get the pick selection data, execute if exists
    if pick_selection_title := data.get('pickSelectionTitle'):
        # get additional data from pick selection, execute if exists
        if additional_options := pick_selection_title.get('additionalOptions'):
            # get market data and get numeric over/under line, execute if both exist
            if (stat_text := additional_options.get('statText')) and (line := additional_options.get('line')):
                # return the numeric line for the prop line and some additional market data
                return line, stat_text


def extract_market(data: list, league: str) -> Optional[tuple[Optional[str], str]]:
    # markets are formatted in a particular ways
    stat_text_components = data[0].split('.')
    # checks if formatting quirk holds
    if len(stat_text_components) == 4:
        # extracts the actual market from components
        if market := stat_text_components[-2].lower().title():
            # cleans the market
            market = clean_market(market)
            # gets the market id
            market_id = get_market_id(Market(market, league))
            # return both market and market id
            return market_id, market


def extract_label(data: dict) -> Optional[str]:
    # get the label, execute formatting if exists
    if label := data.get('type'):
        # capitalize the label
        return label.title()


class BoomFantasy(Plug):
    """
    BoomFantasy is a class that represents the process of collecting and parsing player prop lines from
    the BoomFantasy API. It inherits from the `Plug` class and utilizes asynchronous requests to gather
    data related to prop betting markets.

    Attributes:
        info (Bookmaker): An object containing information about the bookmaker.
        batch_id (str): A unique identifier for the batch of data being processed.
        req_mngr (RequestManager): Manages network requests, including handling authentication tokens.

    Methods:
        start(): Asynchronously starts the data collection process by sending a POST request to the
            BoomFantasy API to retrieve prop lines using authentication tokens.

        _parse_lines(response): Parses the API response to extract key information such as the league,
            player subjects, markets, and prop lines. This information is formatted and added to shared
            data storage for further use.
    """

    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def start(self):
        # gets the url to get prop lines
        url = self.req_packager.get_url()
        # gets json data that will be sent with request for prop lines
        json_data = self.req_packager.get_json_data()
        # gets valid tokens needed to access data
        tokens = read_tokens()
        # because of tokens, use a special posting method to request data
        await self.req_mngr.post_bf(url, self._parse_lines, file_path=tokens['path'],
                                    refresh_token=tokens['refresh_token'], access_token=tokens['access_token'],
                                    json_data=json_data)

    async def _parse_lines(self, response):
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # get the contest from the data, executes if exists
            if contest := data.get('contest'):
                # for each section in the contest's sections if they exist
                for section in contest.get('sections', []):
                    # extract data about the league and the league name
                    league_data, league = extract_league(section)
                    # if they exist execute
                    if league_data and league:
                        # checks if the league is valid
                        if not is_league_valid(league):
                            continue

                        # for each section in the league's sections if they exist
                        for league_section in league_data.get('sections', []):
                            # extract the subject and get the subject id from the response data and database
                            subject_id, subject = extract_subject(league_section, league)
                            # if they both exist then execute
                            if subject_id and subject:
                                # get more prop line info from the league's section's fullQuestions if they exist
                                for question in league_section.get('fullQuestions', []):
                                    # extract the numeric line for the prop line and some additional market data
                                    line, market_data = extract_line(question)
                                    # extract the market and market id from the response data and database
                                    market_id, market = extract_market(market_data, league)
                                    # if they both exist then execute
                                    if market_id and market:
                                        # for each over or under side to the prop line if they exist.
                                        for choice in question.get('choices', []):
                                            # extract the label from the dictionary, if exists then execute
                                            if label := extract_label(choice):
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
                                                    'odds': self.info.default_payout.odds
                                                })


if __name__ == "__main__":
    asyncio.run(run(BoomFantasy))
