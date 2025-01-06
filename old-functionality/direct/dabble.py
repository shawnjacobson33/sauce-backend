import asyncio
import json
from typing import Optional

from app.cache import redis_cache as r
from app.services.betting_lines.data_collection.utils import fetch


r.data_providers.storeproviders([
    {
        'name': 'Dabble',
        'urls': {
            'competitions': 'https://api.dabble.com/competitions/active/',
            'events': 'https://api.dabble.com/competitions/{}/sport-fixtures',
            'prop_lines': 'https://api.dabble.com/sportfixtures/details/{}'
        },
        'headers': {
            'all': {
                'Host': 'api.dabble.com',
                'accept': 'application/json',
                'user-agent': 'Dabble/72 CFNetwork/1568.200.51 Darwin/24.1.0',
                'accept-language': 'en-US,en;q=0.9',
            },
        },
        'params': {
            'events': {
                'exclude[]': [
                    'markets',
                    'selections',
                    'prices',
                ],
            },
            'prop_lines': {
                'filter': 'dfs-enabled',
            }
        },
    }
])



logistics = r.data_providers.getprovider('Dabble')

prop_lines = []


async def _request_competitions() -> None:
    url = logistics['urls']['competitions']
    headers = logistics['headers']['all']
    if resp_json := await fetch(url, headers=headers):
        await _parse_competitions(resp_json)


def _extract_league(competition: dict) -> Optional[str]:
    if raw_league_name := competition.get('displayName'):
        return r.leagues.getleague(raw_league_name)


async def _parse_competitions(resp: dict) -> None:
    if data := resp.get('data'):
        tasks = []
        for competition in data.get('activeCompetitions', []):
            if (league := _extract_league(competition)) and (competition_id := competition.get('id')):
               tasks.append(_request_events(league, competition_id))

        await asyncio.gather(*tasks)


async def _request_events(league: str, competition_id: str) -> None:
    url = logistics['urls']['events'].format(competition_id)
    params = logistics['params']['events']
    if resp_json := await fetch(url, params=params):
        await _parse_events(league, resp_json)


async def _parse_events(league: str, resp: dict) -> None:
    tasks = []
    for event in resp.get('data', []):
        if (event_id := event.get('id')) and event.get('isDisplayed'):
            tasks.append(_request_prop_lines(league, event_id))

    await asyncio.gather(*tasks)


async def _request_prop_lines(league: str, event_id: str):
    url = logistics['urls']['prop_lines'].format(event_id)
    params = logistics['params']['prop_lines']
    if resp_json := await fetch(url, params=params):
        _parse_prop_lines(league, resp_json)


def _extract_market_map_helper(markets: dict, market_name: str, data: dict) -> None:
    for market in data.get('markets', []):
        if market_id := market.get('id'):
            markets[market_id] = market_name


def _extract_market_map(data: dict) -> Optional[dict]:
    markets = dict()
    for market_data in data.get('marketGroupMappings', []):
        if market_name := market_data.get('name'):
            _extract_market_map_helper(markets, market_name, market_data)

    return markets


def _extract_market(league: str, player_prop: dict, markets_map: dict) -> Optional[str]:
    if (market_id := player_prop.get('marketId')) and (raw_market_name := markets_map.get(market_id)):
        if sport := r.leagues.getsport(league):
            return r.markets.getmarket(raw_market_name, sport)


def _extract_team(player_prop: dict) -> Optional[tuple[str, str]]:
    if raw_team_name := player_prop.get('teamAbbreviation'):
        return r.teams.getteam(raw_team_name)


def _extract_game(player_prop: dict) -> Optional[dict]:
    if team := _extract_team(player_prop):
        return r.games.getgame(team)


def _extract_subject(player_prop: dict) -> Optional[dict]:
    if team := _extract_team(player_prop):
        if raw_subject_name := player_prop.get('playerName'):
            return r.subjects.getsubject(raw_subject_name, team)


def _parse_prop_lines(league: str, resp: dict) -> None:
    global prop_lines
    if data := resp.get('data'):
        if markets_map := _extract_market_map(data):
            for player_prop in data.get('playerProps', []):
                if market := _extract_market(player_prop, markets_map):
                    if game := _extract_game(player_prop):
                        if subject := _extract_subject(player_prop):
                            if label := player_prop.get('lineType'):
                                if line := player_prop.get('value'):
                                    if odds := r.bookmakers.getdfltodds('Dabble'):
                                        prop_lines.append({
                                            'bookmaker': 'Dabble',
                                            'league': league,
                                            'game': game,
                                            'market': market,
                                            'subject': subject,
                                            'label': label,
                                            'line': line,
                                            'dflt_odds': odds,
                                            'dflt_impl_prb': round(1 / odds, 4)
                                        })


async def run_dabble_scraper():
    try:
        await _request_competitions()
        # Todo: make sure that the _id for game and subject are both collected
        r.data_lines.storelines(prop_lines)

    except Exception as e:
        print(e)

    finally:
        r.client.hset('providers', logistics['name'], json.dumps(logistics))


if __name__ == '__main__':
    asyncio.run(run_dabble_scraper())