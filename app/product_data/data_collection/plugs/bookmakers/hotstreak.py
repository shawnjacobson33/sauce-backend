import json
import random
from datetime import datetime
import asyncio
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league, clean_position, is_market_valid


def extract_league_aliases(data: dict) -> Optional[dict]:
    if system := data.get('system'):
        return {
            league.get('name'): league.get('alias')
            for sport in system.get('sports', [])
            for league in sport.get('leagues', [])
            if league.get('name') and league.get('alias')
        }


def extract_leagues(data: dict, league_aliases: dict) -> dict:
    # holds league name and league id
    leagues = dict()
    # for each league in the search's leagueFilters if they exist
    for league in data.get('leagueFilters', []):
        # get league id and league name, if both exist then execute
        if (league_id := league.get('key')) and (league_name := league.get('name')):
            # get the league alias from league aliases, if exists set key league id to it, otherwise league name
            leagues[league_id] = league_alias if (
                league_alias := league_aliases.get(league_name)) else league_name

    return leagues



def extract_opponent_ids(data: dict, league_aliases: dict) -> dict:
    # extract a dictionary of leagues connecting league ids to league names
    leagues = extract_leagues(data, league_aliases)
    # holds opponent id and other details about the game
    opponent_ids = dict()
    # for each game in search's games if they exist
    for game in data.get('games', []):
        # get the league id, if exists continue executing
        if league_id := game.get('leagueId'):
            # for each opponent in game's opponents if they exist
            for opponent in game.get('opponents', []):
                # get the opponent id and league name (from league dict above), if exists store key-value pair
                if (opponent_id := opponent.get('id')) and (league := leagues.get(league_id)):
                    # store the key-value pair
                    opponent_ids[opponent_id] = league

    return opponent_ids


def extract_participants(data: dict) -> dict:
    # holds the participant's id and their corresponding subject data
    participants = dict()
    # for each player dict in search's participants if they exist
    for player in data.get('participants', []):
        # get the participant id, and format, and get opponent id, if both exist continue executing
        if (participant_id := player.get('id').split(':')[1]) and (opponent_id := player.get('opponentId')):
            # if the player's data dictionary exists get it and execute
            if player_data := player.get('player'):
                # get the player's name, if exists then execute
                if subject := player_data.get('displayName'):
                    # create key-value pair for participant id and relating attributes
                    participants[participant_id] = {
                        'subject': subject, 'position': player_data.get('position'),
                        'opponent_id': opponent_id, 'jersey_number': player_data.get('number')
                    }

    return participants


def extract_participant_id(data: dict) -> Optional[tuple[str, list]]:
    # get the market data, if they exist then execute
    if market_data := data.get('id'):
        # get the market components by splitting
        market_components = market_data.split(':')
        # check if there are more than one component
        if len(market_components) > 1:
            # break down even further to get the part. id and market name in a list
            all_market_components = ''.join(market_components[1:]).split(',')
            # make sure that there are at least two components
            if len(market_components) > 1:
                # return the participant id and components data structure
                return all_market_components[0], all_market_components


def extract_league(data: dict, opponent_ids: dict) -> Optional[str]:
    # get opponent id and then league from opponent ids dict, if both exist then execute
    if (opponent_id := data.get('opponent_id')) and (league := opponent_ids.get(opponent_id)):
        # clean the league name
        league = clean_league(league)
        # check the validity of the league and if so then return the league name
        if is_league_valid(league):
            # return the cleaned and valid league name
            return league


def extract_position(data: dict) -> Optional[str]:
    # get position from data, if exists then execute
    if position := data.get('position'):
        # return the cleaned up position
        return clean_position(position)


def extract_jersey_number(data: dict) -> Optional[str]:
    # get the jersey number from the data, if exists then execute
    if jersey_number := data.get('jersey_number'):
        # cast it to a string and return it
        return str(jersey_number)


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the subject name from the data, if it exists then execute
    if subject := data.get('subject'):
        # get the cleaned subject
        subject = clean_subject(subject)
        # get subject attributes
        position, jersey_number = extract_position(data), extract_jersey_number(data)
        # get the subject id from the db
        if subject_id := get_subject_id(Subject(subject, league, position=position, jersey_number=jersey_number)):
            # cast to a string
            subject_id = str(subject_id)

        # return the subject id and cleaned subject name
        return subject_id, subject


def extract_market(data: list, league: str) -> Optional[tuple[str, str]]:
    # make sure the data has enough elements to index properly
     if len(data) > 1:
        # get the market name from the data list
        market = data[1]
        # get the cleaned market
        cleaned_market = clean_market(market, 'hot_streak', league=league)
        # get the market id from the db
        if market_id := get_market_id(Market(cleaned_market, league)):
            # cast it to a string
            market_id = str(market_id)

        # get and return the market id from the db along with the market name
        return market_id, cleaned_market


def extract_line(data: dict) -> Optional[str]:
    # get the lines and probabilities from the dictionary, if they both exist then keep going
    if lines := data.get('lines'):
        # for each line
        for line in lines:
            # yield each line
            yield line


def extract_odds(data: dict) -> Optional[tuple[str, str]]:
    # get the probabilities, if exists keep going
    if probs := data.get('probabilities'):
        # for each probability
        for prob in probs:
            # if there are exactly two probabilities corresponding to over/under
            if len(prob) == 2:
                # convert probability to decimal odds and yield for over and under
                yield round(1 / float(prob[0]), 3), round(1 / float(prob[1]), 3)


class HotStreak(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # make call to the parent class Plug
        super().__init__(info, batch_id, req_mngr)
        # get the universal url to make for all requests (uses graphql)
        self.url = self.req_packager.get_url()
        # get the universal headers to make for all requests
        self.headers = self.req_packager.get_headers()

    async def start(self) -> None:
        # get the json data needed for the request for current leagues data
        json_data = self.req_packager.get_json_data(name='leagues')
        # make the request for leagues data
        await self.req_mngr.post(self.url, self._parse_league_aliases, headers=self.headers, json=json_data)

    async def fetch_page(self, leagues: dict, page: int) -> None:
        # get the json_data (for post request) to get the data of a page of prop lines
        json_data = self.req_packager.get_json_data(var=page)
        # make the post request for the particular (page) page of prop lines
        return await self.req_mngr.post(self.url, self._parse_lines, leagues, page, headers=self.headers,
                                                 json=json_data)


    async def _parse_league_aliases(self, response) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # extracts the league aliases from the response data, execute if it exists
            if league_aliases := extract_league_aliases(data):
                # initialize a list of tasks that hold the requests to be made
                tasks = []
                # to not be predictable, and after testing found that page numbers typically go through 50
                for i in range(1, random.randint(45, 50)):
                    # for each page number get the corresponding page request for prop lines
                    tasks.append(self.fetch_page(league_aliases, i))

                # make all the requests asynchronously
                await asyncio.gather(*tasks)

    # TODO: GETTING UNAUTHORIZED RESPONSES FOR PAGES > 1
    async def _parse_lines(self, response, league_aliases: dict, page) -> None:
        # gets the json data from the response and then the redundant data from data field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('data')):
            # get the search dict from data, execute if exists
            if search := data.get('search'):
                # extract participants dict connecting part. ids to subject attributes
                participants = extract_participants(search)
                # for each market in search's markets if they exist
                for market_dict in search.get('markets', []):
                    # extract the participant id and market components from market_dict
                    participant_id, market_components = extract_participant_id(market_dict)
                    # continue executing if both exist and a participant exists in dictionary for given part. id
                    if participant_id and market_components and (participant_data := participants.get(participant_id)):
                        # extract the league using an extracted dict of conn. opp. ids to league names, if exists then execute
                        if league := extract_league(participant_data, extract_opponent_ids(search, league_aliases)):
                            # extract the market id and market name from data
                            market_id, market = extract_market(market_components, league)
                            # if they both exist then execute
                            if market_id and market:
                                # get the subject id from db and extract subject from data
                                subject_id, subject = extract_subject(participant_data, league)
                                # if both exist keep going
                                if subject_id and subject:
                                    # for each line and corresponding over/under odds pair
                                    for line, odds_pair in zip(extract_line(market_dict), extract_odds(market_dict)):
                                        # each (odds) and label are at corresponding indices, so for each of them...
                                        for odds, label in zip(odds_pair, ['Under', 'Over']):
                                            # update shared data
                                            self.add_and_update({
                                                'batch_id': self.batch_id,
                                                'time_processed': str(datetime.now()),
                                                'league': league,
                                                'market_category': 'player_props',
                                                'market_id': market_id,
                                                'market': market,
                                                'subject_id': subject_id,
                                                'subject': subject,
                                                'bookmaker': self.info.name,
                                                'label': label,
                                                'line': line,
                                                'odds': odds
                                            })


if __name__ == "__main__":
    asyncio.run(run(HotStreak))
    Plug.save_to_file()
