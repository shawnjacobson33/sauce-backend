from datetime import datetime
import asyncio
from typing import Optional, Union, Any, Dict

from app.backend.data_collection.bookmakers import utils as bkm_utils


def extract_league(data: dict) -> Optional[str]:
    # get the league name if it exists then execute
    if league := data.get('league_name'):
        # clean the league
        cleaned_league = bkm_utils.clean_league(league)
        # check for validity of the league
        if bkm_utils.is_league_valid(league):
            # return the cleaned league name
            return cleaned_league


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # only want markets that are in the domain of player props
    if data.get('tab_name') == 'Player Props':
        # get the market name and check
        if market_name := data.get('name'):
            # fix some formatting issues specific to Rebet's formatting
            if '{%player}' in market_name:
                # re-formats the market name
                market_name = ' '.join(market_name.split(' (')[0].split()[1:]).title()

            # gets the market id or log message
            market = bkm_utils.get_market_id(bookmaker_name, league, market_name)
            # return both market id search result and cleaned market
            return market


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get subject name from the dictionary, continue executing if it exists
    if subject := data.get('player_name'):
        # needed to extract and reformat the name
        subject_name_components = subject.split(', ')
        # check if the list has at least two elements
        if len(subject_name_components) > 1:
            # get the subject name
            subject_name = f'{subject_name_components[1]} {subject_name_components[0]}'
            # gets the subject id or log message
            return bkm_utils.get_subject(bookmaker_name, league, subject_name)


def extract_odds(data: dict) -> Optional[float]:
    # get the odds for the prop line and check if doesn't equal an invalid value, if exists then execute
    if (odds := data.get('odds')) and (odds != '1.001'):
        # return the odds for the prop line
        return float(odds)


def extract_line_and_label(data: dict) -> Union[dict[str, Any], dict[str, Union[str, Any]]]:
    # get some info that holds label and numeric over/under line data, if exists then execute
    if outcome_info := data.get('name'):
        # split the info into components to extract each individual piece of data
        outcome_info_components = outcome_info.split()
        # condition to check if over or under are in the info and check for valid indexing
        if ('over' in outcome_info) or ('under' in outcome_info) and (len(outcome_info_components) > 1):
            # return the label and line from components
            return {
                'label': outcome_info_components[0].title(),
                'line': float(outcome_info_components[1])
            }
        # condition to check for a way to format an 'over' prop line
        elif '+' in outcome_info:
            # return the label as 'Over' and the line from components
            return {
                'label': 'Over',
                'line': float(outcome_info_components[-1][:-1])
            }


class Rebet(bkm_utils.LinesRetriever):
    def __init__(self, bookmaker: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(bookmaker)

    async def retrieve(self) -> None:
        # get the url required to request tourney ids data
        url = bkm_utils.get_url(self.source.name, name='tourney_ids')
        # get the headers required to request tourney ids data
        headers = bkm_utils.get_headers(self.source.name, name='tourney_ids')
        # get the json data required to request tourney ids data with POST request
        json_data = bkm_utils.get_json_data(self.source.name, name='tourney_ids')
        # make the post request for tourney ids data
        await self.req_mngr.post(url, self._parse_tourney_ids, headers=headers, json=json_data)

    async def _parse_tourney_ids(self, response) -> None:
        # get response data in json, if exists then keep executing
        if json_data := response.json():
            # initialize a structure to hold requests
            tasks = []
            # for each league data dictionary from response data if exists
            for league_data in json_data.get('data', []):
                # get the tournament id, if exist keep executing
                if tournament_id := league_data.get('tournament_id'):
                    # get the url required to request for prop lines
                    url = bkm_utils.get_url(self.source.name)
                    # get the headers required to request for prop lines
                    headers = bkm_utils.get_headers(self.source.name)
                    # get the json data required to request for prop lines using tournament id
                    json_data = bkm_utils.get_json_data(self.source.name, var=tournament_id)
                    # add the request to tasks
                    tasks.append(self.req_mngr.post(url, self._parse_lines, headers=headers, json=json_data))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # for each event dictionary in data's events if they exist
            for event_data in data.get('events', []):
                # extract league name from the dictionary and get data dic, if both exists keep executing
                if (league := extract_league(event_data)) and (odds_data := event_data.get('odds')):
                    # to track the leagues being collected
                    bkm_utils.Leagues.update_valid_leagues(self.source.name, league)
                    # for each market dictionary in the odds data dictionary's market if they exist
                    for market_data in odds_data.get('market', []):
                        # get the market id from the db and extract the market name from dictionary
                        if market := extract_market(self.source.name, market_data, league):
                            # TODO: For Subjects Shared Data make sure to store a team id so that it can be used to get a game
                            # get the subject id from db, and extract the subject name from dictionary
                            if subject := extract_subject(self.source.name, market_data, league):
                                # use team data to get some game data
                                if game := bkm_utils.get_game_id(league, subject['team_id']):
                                    # get dictionary that holds data on odds, label, line, if exists then execute
                                    if outcomes_data := market_data.get('outcome', []):
                                        # convert to list if outcomes data only returns a dictionary
                                        outcomes_data = [outcomes_data] if not isinstance(outcomes_data, list) else outcomes_data
                                        # for each dictionary in the list of dictionaries
                                        for outcome_data in outcomes_data:
                                            # extract the decimal odds from the dictionary, if exists then execute
                                            if odds := extract_odds(outcome_data):
                                                # if both exist then execute
                                                if bet_details := extract_line_and_label(outcome_data):
                                                    # update shared data
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
                                                        'label': bet_details['label'],
                                                        'line': bet_details['line'],
                                                        'odds': odds,
                                                    })
