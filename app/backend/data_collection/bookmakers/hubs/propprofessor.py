from datetime import datetime
import asyncio
from typing import Optional
import math

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers import utils as bkm_utils


MARKET_MAP = {
    'Basketball': [
        "Moneyline",
        "Total Points",
        "Point Spread",
        "Player Points",
        "Player Assists",
        "Player Rebounds",
        "Player Steals",
        "Player Blocks",
        "Player Points + Assists",
        "Player Points + Rebounds",
        "Player Assists + Rebounds",
        "Player Points + Assists + Rebounds",
        "Player Three Made Shots",
        "Player Points + Rebounds + Assists + Steals",
        "Player Points + Rebounds + Assists + Steals + Blocks",
        "Player Triple Double"
    ],
    'Football': [
        "Moneyline",
        "Point Spread",
        "Total Points",
        "Team Total Points",
        "Player Passing Yards",
        "Player Passing Attempts",
        "Player Passing Completions",
        "Player Passing Interceptions",
        "Player Rushing Yards",
        "Player Rushing Attempts",
        "Player Receiving Yards",
        "Player Receptions",
        "Player Receiving Targets",
        "Player Longest Reception",
        "Player Longest Rush",
        "Player Longest Passing Completion",
        "Player Tackles for Loss",
        "Player Tackles + Assists",
        "Player Sacks",
        "Player Field Goals Made",
        "Player Kicking Points",
        "Player Punts",
        "Player Passing + Rushing Yards",
        "Player Rushing + Receiving Yards",
        "Player Passing + Receiving Yards"
    ],
    'Ice Hockey': [
        "Moneyline",
        "Puck Line",
        "Total Goals",
        "Team Total Goals",
        "Player Assists",
        "Player Goals",
        "Player Points",
        "Player Shots on Goal",
        "Player Hits",
        "Player Blocks",
        "Player Faceoffs Won"
    ]
}
MARKET_TIME_MAP = {
    'Basketball': [
        'Game', '1st Half', '2nd Half', '1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter'
    ],
    'Football': [
        'Game', '1st Half', '2nd Half', '1st Quarter', '2nd Quarter', '3rd Quarter', '4th Quarter'
    ],
    'Ice Hockey': ['Game', '1st Period', '2nd Period', '3rd Period']
}


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the market name, if it exists keep executing
    if market_name := data.get('market'):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get the subject's name from the dictionary, if exists keep going
    if subject_name := data.get('participant'):
        # get subject
        subject = dc_utils.get_subject(bookmaker_name, league, subject_name)
        # return both subject id search result and cleaned subject
        return subject


def convert_to_decimal_odds(american_odds: float) -> float:
    if american_odds < 0:
        return 100 / math.fabs(american_odds) + 1

    return american_odds / 100 + 1


def extract_line_data(data: dict):
    return [
        ('Over' if i == 1 else 'Under', line, convert_to_decimal_odds(american_odds))
        for i in range(1, 3)
        if (line := data.get(f'line_{i}')) if (american_odds := data.get(f'odds_{i}'))
    ]


class PropProfessor(bkm_utils.LinesRetriever):
    def __init__(self, lines_hub: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(lines_hub)

    async def retrieve(self) -> None:
        # initialize a dictionary to hold requests
        tasks = list()
        # get the url to request matchups data
        url = bkm_utils.get_url(self.name, name='matchups')
        # get the headers to request matchups data
        headers = bkm_utils.get_headers(self.name)
        # get the cookies to request matchups data
        cookies = bkm_utils.get_cookies(self.name)
        # get the template for params
        params_templ = bkm_utils.get_params(self.name)
        # for every available league
        for league in dc_utils.IN_SEASON_LEAGUES:
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # for each market that prop professor offers with this sport
            for market in MARKET_MAP[sport]:
                # for every market time that prop professor offers with the sport
                for market_time in MARKET_TIME_MAP[sport]:
                    # inject the params into the template
                    params = params_templ.format(market, league, market_time)
                    # make request for matchups data
                    tasks.append(self.req_mngr.get(
                        url, self._parse_lines, sport, league, headers=headers, cookies=cookies, params=params
                    ))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, sport: str, league: str):
        if (response_json := response.json()) and (result := response_json.get('result')):
            if (data := result.get('data')) and (json_data := data.get('json')):
                dc_utils.RelevantData.update_relevant_leagues(league, self.name)
                for game_data in json_data.get('game_data', []):
                    # get the market id from db and extract market from the data
                    if market := extract_market(self.name, game_data, league):
                        # extract the subject from the db
                        if subject := extract_subject(self.name, game_data, league):
                            # use team data to get some game data
                            if game := dc_utils.get_game(league, subject['team']):
                                for bookmaker, odds_data in game_data.get('odds', {}).items():
                                    for line_data in extract_line_data(odds_data):
                                        self.update_betting_lines({
                                                's_tstamp': str(datetime.now()),
                                                'bookmaker': bookmaker,
                                                'sport': sport,
                                                'league': league,
                                                'game': game['info'],
                                                'market_id': market['id'],
                                                'market': market['name'],
                                                'subject_id': subject['id'],
                                                'subject': subject['name'],
                                                'label': line_data[0],
                                                'line': line_data[1],
                                                'odds': line_data[2],
                                                'im_prb': round(1 / line_data[3], 4),
                                            })








