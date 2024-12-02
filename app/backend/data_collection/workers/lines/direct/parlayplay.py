import asyncio
from datetime import datetime
from typing import Optional, Union, Any

from app.backend.data_collection.workers import utils as dc_utils
from app.backend.data_collection.workers.lines import utils as ln_utils


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position
        return dc_utils.clean_position(position)


def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[dict[str, str]]:
    # get the dictionary holding player's team, if exists execute
    if (team_data := data.get('team')) and (abbr_team_name := team_data.get('teamAbbreviation')):
        # get the team id and team name from the database
        if team_data := dc_utils.get_team(bookmaker_name, league, abbr_team_name):
            # return the team id and team name
            return team_data


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get a dictionary holding player attributes, if exists keep executing
    if subject_name := data.get('fullName'):
        # gets the subject id or log message
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the dictionary that holds market data and get market name, if both exist then execute
    if market_name := data.get('challengeName'):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_odds_and_label(data: dict) -> Union[tuple[Any, Any], tuple[None, None]]:
    # get odds and labels
    odds_and_labels = zip([data.get('decimalPriceOver'), data.get('decimalPriceUnder')], ['Over', 'Under'])
    for odds, label in odds_and_labels:
        if odds:
            yield round(float(odds), 4), label


class ParlayPlay(ln_utils.LinesRetriever):
    def __init__(self, bookmaker: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)
        # get the headers required to make requests for prop lines
        self.headers = ln_utils.get_headers(self.name)

    async def retrieve(self) -> None:
        # get the url that is required to make requests for prop lines
        url = ln_utils.get_url(self.name, name='sports')
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
                            cleaned_league = dc_utils.clean_league(league_name)
                            # check if the league is valid
                            if ln_utils.is_league_valid(cleaned_league):
                                # get the prop lines url
                                url = ln_utils.get_url(self.name)
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
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # to track the leagues being collected
            dc_utils.RelevantData.update_relevant_leagues(league, self.name)
            # for each player in the response data's players if they exist
            for player_data in json_data.get('players', []):
                # get a player dictionary
                if player := player_data.get('player'):
                    # get some player attributes
                    if team := extract_team(self.name, league, player):
                        # get some game data using the team data
                        if game := dc_utils.get_game(league, team['abbr_name']):
                            # get the subject id from db and extract the subject name from a dictionary
                            if subject := extract_subject(self.name, player, league, team):
                                # for each stat dictionary in the player data dictionary if they exist
                                for stat_data in player_data.get('stats', []):
                                    # get the market id from the db and extract the market
                                    if market := extract_market(self.name, stat_data, league):
                                        # get a dictionary of data around market, lines, and odds
                                        if alt_lines_data := stat_data.get('altLines'):
                                            # for each dictionary holding line, odds data if values exist
                                            for line_data in alt_lines_data.get('values', []):
                                                # if the numeric over/under line exists get it and execute
                                                if line := line_data.get('selectionPoints'):
                                                    # for each over and under label
                                                    for odds, label in extract_odds_and_label(line_data):
                                                        betting_line = {
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
                                                            'odds': odds,
                                                            'im_prb': round(1 / odds, 4)
                                                        }
                                                        if is_boosted := stat_data.get('isBoostedPayout'):
                                                            betting_line['is_boosted'] = is_boosted

                                                        # update shared data
                                                        dc_utils.BettingLines.update(betting_line)


