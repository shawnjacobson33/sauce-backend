import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils


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


def extract_subject(bookmaker_name: str, data: dict, league: str, subject_team: str) -> Optional[tuple[str, str]]:
    # get the player's name, if exists then execute
    if subject_name := data.get('player_name'):
        # get player attributes
        position = extract_position(data)
        # create a subject object
        subject_obj = utils.Subject(subject_name, league, team=subject_team, position=position)
        # gets the subject id or log message
        subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
        # return both subject id search result and cleaned subject
        return subject_id, subject_name

    return None, None


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get market name, execute if it exists
    if market_name := data.get('bid_stats_name'):
        # check if the market is valid...watching out for MMA markets
        if utils.is_market_valid(market_name):
            # create a market object
            market_obj = utils.Market(market_name, league=league)
            # gets the market id or log message
            market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
            # return both market id search result and cleaned market
            return market_id, market_name

    return None, None


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
        # get url to make a request for leagues
        url = utils.get_url(self.bookmaker_info.name, name='leagues')
        # get headers to make a request for prop lines
        headers = utils.get_headers(self.bookmaker_info.name, name='leagues')
        # make asynchronous request for prop lines
        await self.req_mngr.get(url, self._parse_leagues, headers=headers)

    async def _parse_leagues(self, response) -> None:
        # get the response data as json and get another dictionary
        if (json_data := response.json()) and (data := json_data.get('data')):
            # create a list to store requests
            tasks = list()
            # for each dictionary of data in entities
            for entity_data in data.get('entities', []):
                # get the league name and id
                if (league_name := entity_data.get('name')) and (league_id := entity_data.get('id')):
                    # clean the league name
                    cleaned_league = utils.clean_league(league_name)
                    # if the league is valid keep going
                    if utils.is_league_valid(cleaned_league):
                        # get the url to get prop lines data and insert the league id
                        url = utils.get_url(self.bookmaker_info.name).format(league_id)
                        # get the headers associated with prop lines requests
                        headers = utils.get_headers(self.bookmaker_info.name)
                        # store some params
                        params = {
                            'page_no': '1'
                        }
                        # add the request to the list of requests
                        tasks.append(self.req_mngr.get(url, self._parse_lines, cleaned_league, headers=headers, params=params))

            # start requesting asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str):
        # get response data, if exists execute
        if json_data := response.json():
            # to track the leagues being collected
            self.metrics.add_league(league)
            # for each event in the data's entities
            for event_data in json_data.get('entities', []):
                # check if the event is valid before executing
                if is_event_valid(event_data):
                    # extract the player's team and some general game info from 'event'
                    game_info = extract_game_info(event_data)
                    # extract the player's team
                    subject_team = extract_subject_team(event_data)
                    # for each player in event's players if they exist
                    for player_data in event_data.get('players', []):
                        # extract the subject id from db and get subject from player dict
                        subject_id, subject_name = extract_subject(self.bookmaker_info.name, player_data, league, subject_team)
                        # only execute if both exist
                        if subject_id and subject_name:
                            # to track the subjects being collected
                            self.metrics.add_subject((league, subject_name))
                            # get market id from db and extract market from player dict
                            market_id, market_name = extract_market(self.bookmaker_info.name, player_data, league)
                            # only execute if both exist
                            if market_id and market_name:
                                # to track the markets being collected
                                self.metrics.add_market((league, market_name))
                                # get numeric over/under line and execute if exists
                                if line := player_data.get('bid_stats_value'):
                                    # for each label Over and Under update shared data
                                    for label in ['Over', 'Under']:
                                        # update shared data
                                        self.update_betting_lines({
                                            'batch_id': self.batch_id,
                                            'time_processed': str(datetime.now()),
                                            'league': league,
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
    utils.run(Drafters)
