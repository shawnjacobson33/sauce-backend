import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.helpers import run, clean_market, clean_subject, clean_position, \
    is_market_valid


def is_event_valid(data: dict) -> bool:
    # do not want futures
    return 'Season' not in data.get('_id')


def extract_team(data: dict) -> Optional[tuple[str, str]]:
    # get the home team and away team from data, if they both exist then execute
    if (home_team := data.get('home')) and (away_team := data.get('away')):
        # get the team the player is on and only return if it exists and doesn't equal MMA
        if (subject_team := data.get('own')) and (subject_team != 'MMA'):
            # return the player's team and some general game info
            return subject_team, ' @ '.join([away_team, home_team])


def extract_subject(data: dict, team: str) -> Optional[tuple[str, str]]:
    # get the player's name, if exists then execute
    if subject := data.get('player_name'):
        # clean the player's name
        subject = clean_subject(subject)
        # return a subject id from the database and the cleaned subject
        return get_subject_id(Subject(subject, team=team, position=extract_position(data)), user='Drafters'), subject


def extract_market(data: dict) -> Optional[tuple[str, str]]:
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
        return clean_position(position.strip())


class Drafters(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        super().__init__(info, batch_id, req_mngr)

    async def start(self):
        # get url to make a request for prop lines
        url = self.req_packager.get_url()
        # get headers to make a request for prop lines
        headers = self.req_packager.get_headers()
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
                    subject_team, game_info = extract_team(event)
                    # for each player in event's players if they exist
                    for player in event.get('players', []):
                        # extract the subject id from db and get subject from player dict
                        subject_id, subject = extract_subject(player, subject_team)
                        # only execute if both exist
                        if subject_id and subject:
                            # get market id from db and extract market from player dict
                            market_id, market = extract_market(player)
                            # only execute if both exist
                            if market_id and market:
                                # get numeric over/under line and execute if exists
                                if line := player.get('bid_stats_value'):
                                    # for each label Over and Under update shared data
                                    for label in ['Over', 'Under']:
                                        # update shared data
                                        self.add_and_update({
                                            'batch_id': self.batch_id,
                                            'time_processed': datetime.now(),
                                            'market_category': 'player_props',
                                            'market_id': market_id,
                                            'market': market,
                                            'game_info': game_info,
                                            'subject_id': subject_id,
                                            'subject': subject,
                                            'bookmaker': self.info.name,
                                            'label': label,
                                            'line': line,
                                            'odds': self.info.default_payout.odds
                                        })


if __name__ == "__main__":
    asyncio.run(run(Drafters))
