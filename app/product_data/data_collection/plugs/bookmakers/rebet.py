from datetime import datetime
import asyncio
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils


def extract_league(data: dict) -> Optional[str]:
    # get the league name if it exists then execute
    if league := data.get('league_name'):
        # clean the league
        cleaned_league = utils.clean_league(league)
        # check for validity of the league
        if utils.is_league_valid(league):
            # return the cleaned league name
            return cleaned_league


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # only want markets that are in the domain of player props
    if data.get('tab_name') == 'Player Props':
        # get the market name and check
        if market_name := data.get('name'):
            # fix some formatting issues specific to Rebet's formatting
            if '{%player}' in market_name:
                # re-formats the market name
                market_name = ' '.join(market_name.split(' (')[0].split()[1:]).title()

            # create a market object
            market_obj = utils.Market(market_name, league=league)
            # gets the market id or log message
            market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
            # return both market id search result and cleaned market
            return market_id, market_name

    return None, None


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get subject name from the dictionary, continue executing if it exists
    if subject := data.get('player_name'):
        # needed to extract and reformat the name
        subject_name_components = subject.split(', ')
        # check if the list has at least two elements
        if len(subject_name_components) > 1:
            # create a subject object
            subject_obj = utils.Subject(f'{subject_name_components[1]} {subject_name_components[0]}', league)
            # gets the subject id or log message
            subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
            # return both subject id search result and cleaned subject
            return subject_id, subject_name

    return None, None


def extract_odds(data: dict) -> Optional[str]:
    # get the odds for the prop line and check if doesn't equal an invalid value, if exists then execute
    if (odds := data.get('odds')) and (odds != '1.001'):
        # return the odds for the prop line
        return odds


def extract_line_and_label(data: dict) -> Optional[tuple[str, str]]:
    # get some info that holds label and numeric over/under line data, if exists then execute
    if outcome_info := data.get('name'):
        # split the info into components to extract each individual piece of data
        outcome_info_components = outcome_info.split()
        # condition to check if over or under are in the info and check for valid indexing
        if ('over' in outcome_info) or ('under' in outcome_info) and (len(outcome_info_components) > 1):
            # return the label and line from components
            return outcome_info_components[0].title(), outcome_info_components[1]
        # condition to check for a way to format an 'over' prop line
        elif '+' in outcome_info:
            # return the label as 'Over' and the line from components
            return 'Over', outcome_info_components[-1][:-1]


class Rebet(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url required to request tourney ids data
        url = utils.get_url(self.bookmaker_info.name, name='tourney_ids')
        # get the headers required to request tourney ids data
        headers = utils.get_headers(self.bookmaker_info.name, name='tourney_ids')
        # get the json data required to request tourney ids data with POST request
        json_data = utils.get_json_data(self.bookmaker_info.name, name='tourney_ids')
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
                    url = utils.get_url(self.bookmaker_info.name)
                    # get the headers required to request for prop lines
                    headers = utils.get_headers(self.bookmaker_info.name)
                    # get the json data required to request for prop lines using tournament id
                    json_data = utils.get_json_data(self.bookmaker_info.name, var=tournament_id)
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
                    self.metrics.add_league(league)
                    # for each market dictionary in the odds data dictionary's market if they exist
                    for market_data in odds_data.get('market', []):
                        # get the market id from the db and extract the market name from dictionary
                        market_id, market_name = extract_market(self.bookmaker_info.name, market_data, league)
                        # if both exist then keep executing
                        if market_id and market_name:
                            # to track the markets being collected
                            self.metrics.add_market((league, market_name))
                            # get the subject id from db, and extract the subject name from dictionary
                            subject_id, subject_name = extract_subject(self.bookmaker_info.name, market_data, league)
                            # if both exist then keep executing
                            if subject_id and subject_name:
                                # to track the subjects being collected
                                self.metrics.add_subject((league, subject_name))
                                # get dictionary that holds data on odds, label, line, if exists then execute
                                if outcomes_data := market_data.get('outcome', []):
                                    # convert to list if outcomes data only returns a dictionary
                                    outcomes_data = [outcomes_data] if not isinstance(outcomes_data, list) else outcomes_data
                                    # for each dictionary in the list of dictionaries
                                    for outcome_data in outcomes_data:
                                        # extract the decimal odds from the dictionary, if exists then execute
                                        if odds := extract_odds(outcome_data):
                                            # extract the label and numeric over/under line
                                            label, line = extract_line_and_label(outcome_data)
                                            # if both exist then execute
                                            if label and line:
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
                                                })


if __name__ == "__main__":
    utils.run(Rebet)
