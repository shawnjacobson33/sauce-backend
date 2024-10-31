import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league


def extract_league(data: dict) -> Optional[str]:
    # get the league from data, if exists then keep going
    if league := data.get('league'):
        # clean the league
        cleaned_league = clean_league(league)
        # check if league is valid
        if is_league_valid(league):
            # return valid and clean league
            return cleaned_league


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market from data, if exists keep going
    if market_data := data.get('bet_text'):
        # fishes out the market name from the messy string and cleans the market name
        cleaned_market = clean_market(market_data.split(' (')[0], 'money_line')
        # get the market id from the db if it exists
        if market_id := get_market_id(Market(cleaned_market, league)):
            # cast the market id to a string
            market_id = str(market_id)

        # returns the market id from the db and the cleaned market name
        return market_id, cleaned_market


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the subject data from data, if exists keep going
    if subject_data := data.get('title'):
        # splits the data into sub components containing individual attributes
        subject_components = subject_data.split()
        # get the player's name from the subject components, clean the subject
        cleaned_subject = clean_subject(' '.join(subject_components[:-1]))
        # extract the subject team
        subject_team = subject_components[-1][1:-1].replace('r.(', '')
        # get the subject id from the db
        if subject_id := get_subject_id(Subject(cleaned_subject, league, team=subject_team)):
            # cast the subject id to a string
            subject_id = str(subject_id)

        # return the subject id from the database and the cleaned subject name
        return subject_id, cleaned_subject


def extract_line_and_label(data: dict) -> Optional[tuple[str, str]]:
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


class MoneyLine(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def start(self) -> None:
        # gets the url to get prop lines
        url = self.req_packager.get_url()
        # gets the headers to make request for prop lines
        headers = self.req_packager.get_headers()
        # gets the cookies to make request for prop lines
        cookies = self.req_packager.get_cookies()
        # gets the params to make request for prop lines
        params = self.req_packager.get_params()
        # makes request for prop lines
        await self.req_mngr.get(url, self._parse_lines, headers=headers, cookies=cookies, params=params)

    async def _parse_lines(self, response):
        # gets the json data from the response and then the data from bets field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('bets', [])):
            # for each prop line in the data, if they exist
            for prop_line in data:
                # extract the league name, keep going if it exists
                if league := extract_league(prop_line):
                    # extract the market id from database and market name from dictionary
                    market_id, market = extract_market(prop_line, league)
                    # if both exist continue executing
                    if market_id and market:
                        # extract the subject id and subject name from the database and dictionary respectively
                        subject_id, subject = extract_subject(prop_line, league)
                        # if they both exist, continue executing
                        if subject_id and subject:
                            # get line and label for every one that exists
                            for line, label in extract_line_and_label(prop_line):
                                # update shared data
                                self.add_and_update({
                                    'batch_id': self.batch_id,
                                    'time_processed': str(datetime.now()),
                                    'league': league,
                                    'market_category': 'player_props',
                                    'market_id': market_id,
                                    'market': market,
                                    'subject_id': subject_id,
                                    'subject': subject,
                                    'bookmaker': self.info.name,
                                    'label': label,
                                    'line': line,
                                    'odds': self.info.default_payout.odds,
                                    'is_boosted': 'Discount' in market # TODO: COULD BE A BUG HERE
                                })


if __name__ == "__main__":
    asyncio.run(run(MoneyLine))
    Plug.save_to_file()
