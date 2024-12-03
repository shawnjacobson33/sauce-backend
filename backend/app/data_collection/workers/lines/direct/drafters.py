import asyncio
from datetime import datetime
from typing import Optional

from app.backend.data_collection.workers import utils as dc_utils
from app.backend.data_collection.workers.lines import utils as ln_utils


def is_event_valid(data: dict) -> bool:
    # do not want futures
    return 'Season' not in data.get('_id')


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get some event data that holds team data
    if event_data := data.get('event'):
        # get the team the player is on and only return if it exists and doesn't equal MMA
        if (abbr_team_name := event_data.get('own')) and (abbr_team_name != 'MMA'):
            # return the team id and team name
            return dc_utils.get_team(bookmaker_name, league, abbr_team_name)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get the player's name, if exists then execute
    if subject_name := data.get('player_name'):
        # # get player attributes
        # position = extract_position(data)
        # gets the subject id or log message
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get market name, execute if it exists
    if market_name := data.get('bid_stats_name'):
        # check if the market is valid...watching out for MMA markets
        if ln_utils.is_market_valid(market_name):
            # gets the market id or log message
            market = dc_utils.get_market(bookmaker_name, league, market_name)
            # return both market id search result and cleaned market
            return market


def extract_position(data: dict) -> Optional[str]:
    # get position from data if it exists and doesn't equal 'G'
    if (position := data.get('player_position')) and (position != 'G'):
        # return the position cleaned
        return dc_utils.clean_position(position.strip())


class Drafters(ln_utils.LinesRetriever):
    def __init__(self, bookmaker: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)

    async def retrieve(self) -> None:
        # get url to make a request for leagues
        url = ln_utils.get_url(self.name, name='leagues')
        # get headers to make a request for prop lines
        headers = ln_utils.get_headers(self.name, name='leagues')
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
                    cleaned_league = dc_utils.clean_league(league_name)
                    # if the league is valid keep going
                    if ln_utils.is_league_valid(cleaned_league):
                        # get the url to get prop lines data and insert the league id
                        url = ln_utils.get_url(self.name).format(league_id)
                        # get the headers associated with prop lines requests
                        headers = ln_utils.get_headers(self.name)
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
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # to track the leagues being collected
            dc_utils.RelevantData.update_relevant_leagues(league, self.name)
            # for each event in the data's entities
            for event_data in json_data.get('entities', []):
                # check if the event is valid before executing
                if is_event_valid(event_data):
                    # for each player in event's players if they exist
                    for player_data in event_data.get('players', []):
                        # extract the player's team
                        if team := extract_team(self.name, league, player_data):
                            # use the team data to get game data
                            if game := dc_utils.get_game(league, team['abbr_name']):
                                # extract the subject id from db and get subject from player dict
                                if subject := extract_subject(self.name, player_data, league, team):
                                    # get market id from db and extract market from player dict
                                    if market := extract_market(self.name, player_data, league):
                                        # get numeric over/under line and execute if exists
                                        if line := player_data.get('bid_stats_value'):
                                            # for each label Over and Under update shared data
                                            for label in ['Over', 'Under']:
                                                # update shared data
                                                dc_utils.BettingLines.update({
                                                    'batch_id': self.batch_id,
                                                    'bookmaker': self.name,
                                                    'sport': sport,
                                                    'league': league,
                                                    'game_time': game['game_time'],
                                                    'game': game['info'],
                                                    'market_id': market['id'],
                                                    'market': market['name'],
                                                    'subject_id': subject['id'],
                                                    'subject': subject['name'],
                                                    'label': label,
                                                    'line': line,
                                                    'dflt_odds': self.dflt_odds,
                                                    'dflt_im_prb': self.dflt_im_prb
                                                })
