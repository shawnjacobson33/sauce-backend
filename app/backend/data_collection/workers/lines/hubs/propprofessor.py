from datetime import datetime
from typing import Optional
import math

from app.backend.data_collection.workers import utils as dc_utils
from app.backend.data_collection.workers.lines import utils as ln_utils


LEAGUE_MAP = {
    'NCAAM': 'NCAAB'
}
MARKET_MAP = {
    'Basketball': [
        # "Moneyline",
        # "Total Points",
        # "Point Spread",
        # "Team Total Points",
        "Player Points",
        "Player Assists",
        "Player Rebounds",
        "Player Steals",
        "Player Blocks",
        "Player Turnovers",
        "Player Blocks + Steals"
        "Player Points + Assists",
        "Player Points + Rebounds",
        "Player Rebounds + Assists",
        "Player Points + Rebounds + Assists",
        "Player Threes Made",
        "Player Double Double",
        "Player Triple Double"
    ],
    'Football': [
        # "Moneyline",
        # "Point Spread",
        # "Total Points",
        # "Team Total Points",
        # "Team Total Touchdowns",
        "Player Passing Yards",
        "Player Passing Attempts",
        "Player Passing Completions",
        "Player Longest Completion",
        "Player Interceptions",
        "Player Passing Touchdowns",
        "Player Rushing Yards",
        "Player Rushing Attempts",
        "Player Longest Rush",
        "Player Receiving Yards",
        "Player Receptions",
        "Player Receiving Targets",
        "Player Longest Reception",
        "Player Tackles",
        "Player Tackles + Assists",
        "Player Tackles for Loss",
        "Player Tackles Assisted",
        "Player Sacks",
        "Player Field Goals Made",
        "Player Extra Points",
        "Player Kicking Points",
        "Player Punts",
        "Player Touchdowns",
        "Player Passing + Rushing Yards",
        "Player Rushing + Receiving Yards",
    ],
    'Ice Hockey': [
        # "Moneyline",
        # "Puck Line",
        # "Total Goals",
        # "Team Total Goals",
        "Player Assists",
        "Player Goals",
        "Player Points",
        "Player Shots on Goal",
        "Player Hits",
        "Player Blocks",
        "Player Faceoffs Won"
    ],
    'Baseball': [

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
BOOKMAKER_MAP = {
    'Underdog': 'UnderdogFantasy',
    'Underdog (Alt)': 'UnderdogFantasy',
    'Hardrock': 'HardRock',
    'BoomFantasy (Alt)': 'BoomFantasy',
    'ESPN': 'ESPNBet',
    'DraftKings6': 'DraftKingsPick6',
    'NoVigApp': 'NoVig',
    'Betr (Alt)': 'Betr',
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
        ('Over' if i == 1 else 'Under', float(line), convert_to_decimal_odds(float(american_odds)))
        for i in range(1, 3)
        if (line := data.get(f'line_{i}')) if (american_odds := data.get(f'odds_{i}'))
    ]


class PropProfessor(ln_utils.LinesRetriever):
    def __init__(self, lines_hub: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(lines_hub)

    async def retrieve(self) -> None:
        # initialize a dictionary to hold requests
        tasks = list()
        # get the url to request matchups data
        url = ln_utils.get_url(self.name)
        # get the headers to request matchups data
        headers = ln_utils.get_headers(self.name)
        # get the cookies to request matchups data
        cookies = ln_utils.get_cookies(self.name)
        # get the template for params
        params_templ = ln_utils.get_params(self.name)
        # for every available league
        for league in dc_utils.IN_SEASON_LEAGUES:
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # for each market that prop professor offers with this sport
            for market in MARKET_MAP[sport]:
                # # for every market time that prop professor offers with the sport
                # for market_time in MARKET_TIME_MAP[sport]:
                # inject the params into the template
                params_dyn = f'"market":"{market}","league":"{LEAGUE_MAP.get(league, league)}","games":[],"participants":[],"marketTime":"{"Game"}"'
                params_templ['input'] = '{"0":{"json":{' + params_dyn + '}}}'
                # make request for matchups data
                await self.req_mngr.get(
                    url, self._parse_lines, sport, league, headers=headers, cookies=cookies, params=params_templ
                )

        # await asyncio.gather(*tasks)

    async def _parse_lines(self, response, sport: str, league: str):
        if (response_json := response.json()) and (result := response_json[0].get('result')):
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
                                        if len(line_data) == 3:
                                            dc_utils.BettingLines.update({
                                                's_tstamp': str(datetime.now()),
                                                'bookmaker': BOOKMAKER_MAP.get(bookmaker, bookmaker),
                                                'sport': sport,
                                                'league': league,
                                                'game_time': game['game_time'],
                                                'game': game['info'],
                                                'market_id': market['id'],
                                                'market': market['name'],
                                                'subject_id': subject['id'],
                                                'subject': subject['name'],
                                                'label': line_data[0],
                                                'line': line_data[1],
                                                'odds': round(line_data[2], 4),
                                                'im_prb': round(1 / line_data[2], 4),
                                            })








