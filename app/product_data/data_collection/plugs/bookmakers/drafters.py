import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug
from app.product_data.data_collection.utils.objects import Subject, Market, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.utils import clean_market, clean_subject, clean_position, \
    is_market_valid


def is_event_valid(data: dict) -> bool:
    # do not want futures
    return 'Season' not in data.get('_id')


def extract_game_info(data: dict) -> Optional[str]:
    # get the home team and away team from data, if they both exist then execute
    if (home_team := data.get('home')) and (away_team := data.get('away')):
        # returning game information
        return ' @ '.join([away_team, home_team])


def extract_subject_team(data: dict) -> Optional[str]:
    # get the team the player is on and only return if it exists and doesn't equal MMA
    if (subject_team := data.get('own')) and (subject_team != 'MMA'):
        # return the player's team
        return subject_team


def extract_subject(bookmaker_name: str, data: dict, subject_team: str) -> Optional[tuple[str, str]]:
    # get the player's name, if exists then execute
    if subject := data.get('player_name'):
        # clean the player's name
        cleaned_subject = clean_subject(subject)
        # return a subject id from the database and the cleaned subject
        return get_subject_id(Subject(cleaned_subject, team=subject_team, position=extract_position(data)), user='Drafters'), cleaned_subject


def extract_market(bookmaker_name: str, data: dict) -> Optional[tuple[str, str]]:
    # get market name, execute if it exists
    if market := data.get('bid_stats_name'):
        # check if the market is valid...watching out for MMA markets
        if is_market_valid(market):
            # clean market name
            market = clean_market(market)
            # return market id from database and a clean market name
            return get_market_id(Market(market)), market


def extract_position(data: dict) -> Optional[str]:
    # get position from data if it exists and doesn't equal 'G'
    if (position := data.get('player_position')) and (position != 'G'):
        # return the position cleaned
        return utils.clean_position(position.strip())


class Drafters(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get url to make a request for prop lines
        url = utils.get_url(self.bookmaker_info.name)
        # get headers to make a request for prop lines
        headers = utils.get_headers(self.bookmaker_info.name)
        # make asynchronous request for prop lines
        await self.req_mngr.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        # get response data, if exists execute
        if json_data := response.json():
            # for each event in the data's entities
            for event in json_data.get('entities', []):
                # check if the event is valid before executing
                if is_event_valid(event):
                    # extract the player's team and some general game info from 'event'
                    game_info = extract_game_info(event)
                    # extract the player's team
                    subject_team = extract_subject_team(event)
                    # for each player in event's players if they exist
                    for player in event.get('players', []):
                        # extract the subject id from db and get subject from player dict
                        subject_id, subject= extract_subject(self.bookmaker_info.nameplayer, subject_team)
                        # only execute if both exist
                        if subject_id and subject:
                            # get market id from db and extract market from player dict
                            market_id, market= extract_market(self.bookmaker_info.nameplayer)
                            # only execute if both exist
                            if market_id and market:
                                # get numeric over/under line and execute if exists
                                if line := player.get('bid_stats_value'):
                                    # for each label Over and Under update shared data
                                    for label in ['Over', 'Under']:
                                        # update shared data
                                        self.update_betting_lines({
                                            'batch_id': self.batch_id,
                                            'time_processed': datetime.now(),
                                            'market_category': 'player_props',
                                            'market_id': str(market_id),
                                            'market': market_name,
                                            'game_info': game_info,
                                            'subject_id': str(subject_id),
                                            'subject': subject_name,
                                            'bookmaker': self.bookmaker_info.name,
                                            'label': label,
                                            'line': line,
                                            'odds': self.bookmaker_info.default_payout.odds
                                        })


if __name__ == "__main__":
    utils.run(Drafters))
