from datetime import datetime
import asyncio
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils
from app.product_data.data_collection.utils import standardizing as std
from app.product_data.data_collection.plugs.bookmakers.utils import setup
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.bookmakers.base import BookmakerPlug
from app.product_data.data_collection.utils.objects import Subject, Market, Bookmaker


def extract_league(data: dict) -> Optional[str]:
    # get the league name, if it exists keep going
    if league := data.get('slug'):
        # check if the cleaned league is valid...still need originally formatted league for params
        if utils.is_league_valid(clean_league(league)):
            # return the valid league name
            return league


def extract_contest_id(data: list) -> Optional[str]:
    # get the first contest name and check whether it is a parlay contest (player props) or not
    if (contest_name := data[0].get('name')) and ('Parlay Contest' in contest_name):
        # return the parlay contest's id if it exists
        return contest_name.get('id')


def extract_teams_dict(data: dict) -> dict:
    # initialize a dictionary to hold team ids and names for player info
    teams_dict = dict()
    # get the home team and away team, if both exist then execute
    if (home_team_data := data.get('home_team')) and (away_team_data := data.get('away_team')):
        # for each team (home and away)
        for team in [home_team_data, away_team_data]:
            # get the team id and team name for each team data dictionary
            if (team_id := team.get('id')) and (team_name := team.get('code')):
                # store the team name associated with a particular team id
                teams_dict[team_id] = team_name.upper()

        return teams_dict


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market name, if exists keep going
    if market := data.get('name'):
        # clean the market name
        cleaned_market = clean_market(market)
        # return the market id from the db and the cleaned market name
        return get_market_id(Market(market, league)), cleaned_market


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists keep executing
    if position := data.get('position'):
        # return the cleaned position
        return utils.clean_position(position)


def extract_subject_team(data: dict, teams_dict: dict) -> Optional[str]:
    # get the subject's team id, if exists keep executing
    if team_id := data.get('team_id'):
        # return, if exists, the subject's team stored in the teams dict
        return teams_dict.get(team_id)


def extract_subject(bookmaker_name: str, data: dict, teams_dict: dict, league: str) -> Optional[tuple[str, str]]:
    # get the subject's name, if it exists keep executing
    if subject := data.get('name'):
        # clean the subject's name
        cleaned_subject = clean_subject(subject)
        # get the subject id from the db
        subject_id = get_subject_id(Subject(cleaned_subject, league, extract_subject_team(data, teams_dict), extract_position(data), data.get('number')))
        # return the subject id from db and cleaned subject namae
        return subject_id, cleaned_subject


class Payday(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)
        # get the universal headers used to make all requests
        self.headers = utils.get_headers(self.bookmaker_info.name)

    async def collect(self) -> None:
        # get the url required to make request for leagues data
        url = utils.get_url(name='leagues')
        # get the params required to make the request for leagues data
        params = utils.get_params(self.bookmaker_info.name, name='leagues')
        # make the request to get leagues data
        await self.req_mngr.get(url, self._parse_leagues, headers=self.headers, params=params)

    async def _parse_leagues(self, response) -> None:
        # get the response data, if exists keep executing
        if json_data := response.json():
            # initialize a structure to hold requests to be made
            tasks = []
            # get the url required to make requests for contests data
            url = utils.get_url(name='contests')
            # for each dictionary of league data in the response data if it exists
            for league_data in json_data.get('data', []):
                # extract the league from league_data
                if league := extract_league(league_data):
                    # get the params necessary for requesting contests data for this league
                    params = utils.get_params(self.bookmaker_info.name, name='contests', var_1=league)
                    # add the request to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_contests, clean_league(league), headers=self.headers, params=params))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_contests(self, response, league: str) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # get contests dictionary and the contest id from it, if both exists continue executing
            if (contests := data.get('contests')) and (contest_id := extract_contest_id(contests)):
                # get the url required to request for prop lines using the contest id
                url = utils.get_url(self.bookmaker_info.name).format(contest_id)
                # make request for prop lines
                await self.req_mngr.get(url, self._parse_lines, league, headers=self.headers)

    async def _parse_lines(self, response, league: str):
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # for each game in data's games if they exist
            for game_data in data.get('games', []):
                # get game info from dictionary
                game_info = game_data.get('title')
                # get the teams dictionary holding team info
                teams_dict = extract_teams_dict(game_data)
                # for each player prop data dictionary in game_data's player props if they exist
                for player_prop_data in game_data.get('player_props', []):
                    # get the market id from the db and extract the market name
                    market_id, market= extract_market(self.bookmaker_info.nameplayer_prop_data, league)
                    # only keep going if the both exist
                    if market_id and market:
                        # get some player dictionary and the numeric over/under line, if both exist keep executing
                        if (player_data := player_prop_data.get('player')) and (line := player_prop_data.get('value')):
                            # get the subject id from the db and extract the subject name
                            subject_id, subject= extract_subject(self.bookmaker_info.nameplayer_data, teams_dict, league)
                            # if both exist, then keep executing
                            if subject_id and subject:
                                # for each general prop line label
                                for label in ['Over', 'Under']:
                                    # update shared data
                                    self.update_betting_lines({
                                        'batch_id': self.batch_id,
                                        'time_processed': datetime.now(),
                                        'league': league,
                                        'game_info': game_info,
                                        'market_category': 'player_props',
                                        'market_id': str(market_id),
                                        'market': market_name,
                                        'subject_id': str(subject_id),
                                        'subject': subject_name,
                                        'bookmaker': self.bookmaker_info.name,
                                        'label': label,
                                        'line': line,
                                        'odds': self.bookmaker_info.default_payout.odds
                                    })


if __name__ == "__main__":
    utils.run(Payday))
