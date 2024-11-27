from datetime import datetime
import asyncio
from typing import Optional

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers import utils as bkm_utils


def extract_league(data: dict) -> Optional[str]:
    # get the league, if exists then execute
    if league := data.get('sport'):
        # return the cleaned league name
        return dc_utils.clean_league(league)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get some market dictionary and then the market name from it, if both exist keep going
    if (market_type := data.get('marketType')) and (market_name := market_type.get('name')):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists keep executing
    if position := data.get('position'):
        # return the cleaned position string
        return dc_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the subject team, if exists then keep executing
    if abbr_team_name := data.get('teamAlias'):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team(bookmaker_name, league, abbr_team_name.upper()):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get the first nam and last name of the player, if both exist then keep executing
    if (first_name := data.get('firstName')) and (last_name := data.get('lastName')):
        # get subject name
        subject_name = ' '.join([first_name, last_name])
        # return both subject id search result and cleaned subject
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


def extract_line(data: dict) -> Optional[str]:
    # get some dictionary that holds the numeric line, if exists then execute
    if balanced_line := data.get('line'):
        # return the numeric over/under line
        return balanced_line.get('balancedLine')


def extract_labels(label: str) -> Optional[list[str]]:
    # split up the different components of the label string by underscore
    label_comps =  label.split('_')
    # make sure there are enough elements to index
    if len(label_comps) > 1:
        # get the labels based on other keyword in the label components (MORE, OR, LESS....MORE, ONLY)
        connecting_word = label_comps[1]
        # if the word is 'or'
        if connecting_word == 'OR':
            # return both labels in a list
            return ['Over', 'Under']

        # if the word is 'only'
        elif connecting_word == 'ONLY':
            # get the label as 'Over' keyword is 'MORE' else 'Under'
            formatted_label = 'Over' if label_comps[0] == 'MORE' else 'Under'
            # return the singular label in a list
            return [formatted_label]


def get_label(data: dict) -> Optional[str]:
    # get some dictionary holding data about whether the prop line is over/under type of pick, if exists then execute
    if pick_options := data.get('pickOptions'):
        # iterate over each general label for a prop line
        for label in extract_labels(pick_options):
            # yield the label
            yield label


class OwnersBox(bkm_utils.LinesRetriever):
    def __init__(self, bookmaker: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)
        # get the universal headers required to make requests
        self.headers = bkm_utils.get_headers(self.source.name)
        # get the universal cookies required to make requests
        self.cookies = bkm_utils.get_cookies(self.source.name)

    async def retrieve(self) -> None:
        # get the url required to make request for leagues data
        url = bkm_utils.get_url(self.source.name, name='leagues')
        # make the request for the leagues data
        await self.req_mngr.get(url, self._parse_leagues, headers=self.headers, cookies=self.cookies)

    async def _parse_leagues(self, response) -> None:
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # initialize structure to hold all requests to make
            tasks = []
            # get the url required to make requests for markets data
            url = bkm_utils.get_url(self.source.name, name='markets')
            # for each league in the response data dictionary
            for league in json_data:
                # only execute if the league is valid...also clean the league before checking
                if bkm_utils.is_league_valid(dc_utils.clean_league(league)):
                    # if valid get the params to make the request required to get markets data for this league
                    params = bkm_utils.get_params(self.source.name, name='markets', var_1=league)
                    # add the request to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_markets, league, headers=self.headers, cookies=self.cookies, params=params))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_markets(self, response, league: str) -> None:
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # initialize structure to hold all requests to make
            tasks = []
            # get the url required to make the request for prop lines
            url = bkm_utils.get_url(self.source.name)
            # for each market in the response data
            for market in json_data:
                # get market id (not from db), if exists add request
                if market_id := market.get('id'):
                    # get the params required to make request for prop lines
                    params = bkm_utils.get_params(self.source.name, var_1=league, var_2=market_id)
                    # add the request to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_lines, headers=self.headers, cookies=self.cookies, params=params))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response) -> None:
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # for each prop line in response data's markets if they exist
            for prop_line_data in json_data.get('markets', []):
                # get the league name, if exists then keep executing
                if league := extract_league(prop_line_data):
                    # to track the leagues being collected
                    dc_utils.RelevantData.update_relevant_leagues(league, self.source.name)
                    # get the market id from db and extract the market name
                    if market := extract_market(self.source.name, prop_line_data, league):
                        # get the player data, if it exists then keep executing
                        if player_data := prop_line_data.get('player'):
                            # extract team data from the dictionary
                            if team := extract_team(self.source.name, league, player_data):
                                # get game data using the team data
                                if game := dc_utils.get_game(league, team['id']):
                                    # get the subject id from db and extract the subject name from the dictionary
                                    if subject := extract_subject(self.source.name, player_data, league, team):
                                        # get the numeric over/under line, execute if exists
                                        if line := extract_line(prop_line_data):
                                            # for each label that the prop line has
                                            for label in get_label(prop_line_data):
                                                # update the shared data
                                                self.update_betting_lines({
                                                    'batch_id': self.batch_id,
                                                    'time_processed': datetime.now(),
                                                    'bookmaker': self.source.name,
                                                    'league': league,
                                                    'game_id': game['id'],
                                                    'game': game['info'],
                                                    'market_category': 'player_props',
                                                    'market_id': market['id'],
                                                    'market': market['name'],
                                                    'subject_id': subject['id'],
                                                    'subject': subject['name'],
                                                    'label': label,
                                                    'line': float(line),
                                                    'odds': self.source.default_payout.odds
                                                })
