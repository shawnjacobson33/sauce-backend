import asyncio
from datetime import datetime
from typing import Optional, Union, Any

from app import database as db
from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import utils as bkm_utils


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position
        return bkm_utils.clean_position(position)


def extract_subject_team(data: dict) -> Optional[str]:
    # get the dictionary holding player's team, if exists execute
    if team_data := data.get('team'):
        # return the player's abbreviated team name
        return team_data.get('teamAbbreviation')


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get a dictionary holding player attributes, if exists keep executing
    if (player_data := data.get('player')) and (subject_name := player_data.get('fullName')):
        # get some player attributes
        subject_team, position = extract_subject_team(player_data), extract_position(player_data)
        # create a subject object
        subject_obj = dc_utils.Subject(subject_name, league, team=subject_team, position=position)
        # gets the subject id or log message
        subject_id, subject_name = bkm_utils.get_subject_id(bookmaker_name, subject_obj)
        # return both subject id search result and cleaned subject
        return subject_id, subject_name

    return None, None


def extract_market(bookmaker_name: str, data: dict, league: str) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get the dictionary that holds market data and get market name, if both exist then execute
    if market_name := data.get('challengeName'):
        # create a market object
        market_obj = dc_utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = bkm_utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name

    return None, None


def extract_odds_and_label(data: dict) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get odds and labels
    odds_and_labels = zip([data.get('decimalPriceOver'), data.get('decimalPriceUnder')], ['Over', 'Under'])
    for odds, label in odds_and_labels:
        if odds:
            yield odds, label


class ParlayPlay(bkm_utils.BookmakerPlug):
    def __init__(self, bookmaker_info: bkm_utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)
        # get the headers required to make requests for prop lines
        self.headers = bkm_utils.get_headers(self.bookmaker_info.name)

    async def collect(self) -> None:
        # get the url that is required to make requests for prop lines
        url = bkm_utils.get_url(self.bookmaker_info.name, name='sports')
        # make the request for prop lines
        await self.req_mngr.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response) -> None:
        # get the response data as json
        if json_data := response.json():
            # store all tasks to be requested
            tasks = list()
            # for each dictionary of sports data
            for sport_data in json_data.get('sports', []):
                # get the sport name
                if sport_name := sport_data.get('sportName'):
                    # for each dictionary of leagues data
                    for league_data in sport_data.get('leagues', []):
                        # get the league name, if it exists keep going
                        if league_name := league_data.get('leagueNameShort'):
                            # clean the league
                            cleaned_league = bkm_utils.clean_league(league_name)
                            # check if the league is valid
                            if bkm_utils.is_league_valid(cleaned_league):
                                # get the prop lines url
                                url = bkm_utils.get_url(self.bookmaker_info.name)
                                # include some params based upon the data collected
                                params = {
                                    'sport': sport_name,
                                    'league': league_name,
                                    'includeAlt': 'true',
                                    'version': 2
                                }
                                # add the request task to the list of tasks
                                tasks.append(self.req_mngr.get(url, self._parse_lines, cleaned_league, headers=self.headers, params=params))

            # make all requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # get the response data, if exists then keep executing
        if json_data := response.json():
            # to track the leagues being collected
            self.metrics.add_league(league)
            # for each player in the response data's players if they exist
            for player_data in json_data.get('players', []):
                # get the subject id from db and extract the subject name from a dictionary
                subject_id, subject_name = extract_subject(self.bookmaker_info.name, player_data, league)
                # if both subject id and subject exist then keep executing
                if subject_id and subject_name:
                    # to track the subjects being collected
                    self.metrics.add_subject((league, subject_name))
                    # for each stat dictionary in the player data dictionary if they exist
                    for stat_data in player_data.get('stats', []):
                        # get the market id from the db and extract the market
                        market_id, market_name = extract_market(self.bookmaker_info.name, stat_data, league)
                        # if both exist then keep executing
                        if market_id and market_name:
                            # to track the markets being collected
                            self.metrics.add_market((league, market_name))
                            # get a dictionary of data around market, lines, and odds
                            if alt_lines_data := stat_data.get('altLines'):
                                # for each dictionary holding line, odds data if values exist
                                for line_data in alt_lines_data.get('values', []):
                                    # if the numeric over/under line exists get it and execute
                                    if line := line_data.get('selectionPoints'):
                                        # for each over and under label
                                        for odds, label in extract_odds_and_label(line_data):
                                            # update shared data
                                            self.update_betting_lines({
                                                'batch_id': self.batch_id,
                                                'time_processed': str(datetime.now()),
                                                'league': league,
                                                'market_category': 'player_props',
                                                'market_id': str(market_id),
                                                'market': market_name,
                                                'subject_id': str(subject_id),
                                                'subject': subject_name,
                                                'bookmaker': self.bookmaker_info.name,
                                                'label': label,
                                                'line': line,
                                                'odds': odds,
                                                'is_boosted': stat_data.get('isBoostedPayout')
                                            })
