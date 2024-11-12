import asyncio
from datetime import datetime
from typing import Optional, Union, Any

from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import utils as bkm_utils


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position
        return bkm_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the dictionary holding player's team, if exists execute
    if (team_data := data.get('team')) and (team_name := team_data.get('teamAbbreviation')):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team_id(bookmaker_name, league, team_name):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict, position: Optional[str]) -> Optional[dict[str, str]]:
    # get a dictionary holding player attributes, if exists keep executing
    if subject_name := data.get('fullName'):
        # gets the subject id or log message
        subject = bkm_utils.get_subject_id(bookmaker_name, league, subject_name, team=team, position=position)
        # return both subject id search result and cleaned subject
        return subject


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the dictionary that holds market data and get market name, if both exist then execute
    if market_name := data.get('challengeName'):
        # gets the market id or log message
        market = bkm_utils.get_market_id(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_odds_and_label(data: dict) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get odds and labels
    odds_and_labels = zip([data.get('decimalPriceOver'), data.get('decimalPriceUnder')], ['Over', 'Under'])
    for odds, label in odds_and_labels:
        if odds:
            yield odds, label


class ParlayPlay(bkm_utils.LinesRetriever):
    def __init__(self, bookmaker: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)
        # get the headers required to make requests for prop lines
        self.headers = bkm_utils.get_headers(self.source.name)

    async def retrieve(self) -> None:
        # get the url that is required to make requests for prop lines
        url = bkm_utils.get_url(self.source.name, name='sports')
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
                                url = bkm_utils.get_url(self.source.name)
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
            bkm_utils.Leagues.update_valid_leagues(self.source.name, league)
            # for each player in the response data's players if they exist
            for player_data in json_data.get('players', []):
                # get a player dictionary
                if player := player_data.get('player'):
                    # get some player attributes
                    team, position = extract_team(self.source.name, league, player_data), extract_position(player_data)
                    # get some game data using the team data
                    if game := bkm_utils.get_game_id(team):
                        # get the subject id from db and extract the subject name from a dictionary
                        if subject := extract_subject(self.source.name, player, league, team, position):
                            # for each stat dictionary in the player data dictionary if they exist
                            for stat_data in player_data.get('stats', []):
                                # get the market id from the db and extract the market
                                if market := extract_market(self.source.name, stat_data, league):
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
                                                        'time_processed': datetime.now(),
                                                        'league': league,
                                                        'game_id': game['id'],
                                                        'game': game['info'],
                                                        'market_category': 'player_props',
                                                        'market_id': market['id'],
                                                        'market': market['name'],
                                                        'subject_id': subject['id'],
                                                        'subject': subject['name'],
                                                        'bookmaker': self.source.name,
                                                        'label': label,
                                                        'line': line,
                                                        'odds': odds,
                                                        'is_boosted': stat_data.get('isBoostedPayout')
                                                    })
