import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional, Iterable

from app.cache import redis_cache as r
from app.tasks.lines.utils import fetch


r.data_providers.storeproviders([
    {
        'name': 'OddsShopper',
        'urls': {
            'processing_info': 'https://www.oddsshopper.com/api/processingInfo/all',
            'matchups': 'https://www.oddsshopper.com/api/liveOdds/offers?league=all',
            'prop_lines': 'https://api.oddsshopper.com/api/offers/{}/outcomes/live?'
        },
        'headers': {
            'all': {
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Origin': 'https://www.oddsshopper.com',
                'Referer': 'https://www.oddsshopper.com/',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-site',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"macOS"',
            },
        },
        'cookies': {
            '_gcl_au': '1.1.1623273209.1722653498',
            '_ga': 'GA1.1.578395929.1722653499',
            '_clck': 'sq3dhw%7C2%7Cfo0%7C0%7C1676',
            'advanced_ads_page_impressions': '%7B%22expires%22%3A2038059248%2C%22data%22%3A1%7D',
            'advanced_ads_browser_width': '1695',
            '_omappvp': '3R4fJFIbQqbzViRUPUXzhsn7uJbFjZ4ZycGfR5dL7cVUJS3AMw6TzfPLv0ZqsQycti3fiWi7kzDa8w4T9Tev789cLZG3Vzfq',
            '_hp2_id.2282576763': '%7B%22userId%22%3A%225144768874446519%22%2C%22pageviewId%22%3A%223326899841379432%22%2C%22sessionId%22%3A%221530461341951255%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
            '_hp2_ses_props.2436895921': '%7B%22ts%22%3A1722717777024%2C%22d%22%3A%22www.oddsshopper.com%22%2C%22h%22%3A%22%2Fliveodds%2Ftennis%22%7D',
            '_rdt_uuid': '1722653498619.4f5023ed-9e39-40e5-8323-7ed52f08d73c',
            '_clsk': 'ojlg6i%7C1722719433713%7C6%7C1%7Cw.clarity.ms%2Fcollect',
            '_ga_FF6BGPF4L9': 'GS1.1.1722717780.9.1.1722719608.0.0.0',
            '_ga_ZR0H6RP7T1': 'GS1.1.1722717778.9.1.1722719608.60.0.1437947439',
            '_hp2_id.2436895921': '%7B%22userId%22%3A%226206737379708108%22%2C%22pageviewId%22%3A%224229154290207751%22%2C%22sessionId%22%3A%228372167046692848%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
        },
        'params': {
            'prop_lines': {
                'state': 'NJ',
                'startDate': '{}',
                'endDate': '{}',
                'edgeSportsbooks': 'Circa,FanDuel,Pinnacle',
            }
        },
    }
])



logistics = r.data_providers.getprovider('Dabble')
BOOKMAKER_MAP = {
    'Underdog': 'Underdog Fantasy',
    'Pick6': 'DraftKingsPick6',
}

betting_lines = []


async def _request_matchups() -> None:
    url = logistics['urls']['matchups']
    headers = logistics['headers']['all']
    cookies = logistics['cookies']
    if resp_json := await fetch(url, headers=headers, cookies=cookies):
        await _parse_matchups(resp_json)


def _get_offers(resp: dict) -> Iterable:
    for offer_category in resp.get('offerCategories', []):
        if offer_category.get('name') == 'PlayerProps':
            for offer in offer_category.get('offers', []):
                yield offer


def _extract_league(offer: dict) -> Optional[str]:
    if raw_league_name := offer.get('leagueCode'):
        cleaned_league_name = raw_league_name.strip().upper()
        return r.leagues.getleague(cleaned_league_name)


async def _parse_matchups(resp: dict) -> None:
    tasks = []
    for offer in _get_offers(resp):
        if (league := _extract_league(offer)) and (offer_id := offer.get('id')):
            tasks.append(_request_betting_lines(league, offer_id))

    await asyncio.gather(*tasks)


def _get_dates() -> tuple[str, str]:
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    return (datetime.now().strftime(date_format)[:-3] + 'Z',
            (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z')


def _get_params():
    params = logistics['params']['prop_lines']
    start_date, end_date = _get_dates()
    return {**params, 'startDate': start_date, 'endDate': end_date}


async def _request_betting_lines(league: str, offer_id: str) -> None:
    url = logistics['urls']['prop_lines'].format(offer_id)
    headers = logistics['headers']['all']
    params = _get_params()
    if resp_json := await fetch(url, headers=headers, params=params):
        await _parse_betting_lines(league, resp_json)


def _extract_market(league: str, event: dict) -> Optional[dict]:
    if raw_market_name := event.get('offerName'):
        if sport := r.leagues.getsport(league):
            return r.markets.getmarket(sport, raw_market_name)


def _extract_subject(league: str, event: dict) -> Optional[dict]:
    if (participants := event.get('participants')) and (first_participants := participants[0]):
        if raw_subject_name := first_participants.get('name'):
            return r.subjects.getsubject(league, raw_subject_name)


def _extract_bookmaker(outcome: dict) -> Optional[str]:
    if bookmaker := outcome.get('sportsbookCode'):
        return BOOKMAKER_MAP.get(bookmaker, bookmaker)


def _extract_odds(outcome: dict) -> Optional[float]:
    if odds := outcome.get('odds'):
        return round(odds, 2)


def _extract_tw_prb(data: dict) -> Optional[float]:
    if tw_prob := data.get('trueWinProbability'):
        return round(tw_prob, 3)


def extract_ev(data: dict) -> Optional[float]:
    if ev := data.get('ev'):
        return round(ev, 3)


async def _parse_betting_lines(league: str, resp: dict) -> None:
    global betting_lines
    for event in resp:
        if market := _extract_market(league, event):
            if subject := _extract_subject(league, event):
                if game := r.games.getgame(league, subject['team']):
                    for side in event.get('sides', []):
                        if label := side.get('label'):
                            for outcome in side.get('outcomes', []):
                                if bookmaker_name := _extract_bookmaker(outcome):
                                    if odds := _extract_odds(outcome):
                                        betting_lines.append({
                                            'bookmaker': bookmaker_name,
                                            'league': league,
                                            'game': game,
                                            'market': market,
                                            'subject': subject,
                                            'label': label,
                                            'line': float(outcome.get('line', 0.5)),
                                            'odds': odds,
                                            'impl_prb': round(1 / odds, 4),
                                            'tw_prb': _extract_tw_prb(outcome),
                                            'ev': extract_ev(outcome)
                                        })

async def run():
    try:
        await _request_matchups()
        r.data_lines.storelines(betting_lines)

    except Exception as e:
        print(e)

    finally:
        r.client.hset('providers', logistics['name'], json.dumps(logistics))


if __name__ == '__main__':
    asyncio.run(run())
