import asyncio
from datetime import datetime, timedelta
from typing import Optional, Iterable

import pandas as pd

from app.prop_lines.data_collection.utils import fetch, get_payload


payload = get_payload('OddsShopper')
betting_lines = []


IN_SEASON_AND_VALID_LEAGUES = { 'NBA', 'NFL', 'NHL', 'NCAAB', 'NCAAF' }


async def _request_matchups() -> None:
    url = payload['urls']['matchups']
    headers = payload['headers']
    cookies = payload['cookies']
    if resp_json := await fetch(url, headers=headers, cookies=cookies):
        await _parse_matchups(resp_json)


def _get_offers(resp: dict) -> Iterable:
    for offer_category in resp.get('offerCategories', []):
        if offer_category.get('name') == 'PlayerProps':
            for offer in offer_category.get('offers', []):
                yield offer


def _extract_league(offer: dict) -> Optional[str]:
    if raw_league_name := offer.get('leagueCode'):
        if raw_league_name in IN_SEASON_AND_VALID_LEAGUES:
            return raw_league_name.strip().upper()


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
    params = payload['params']['prop_lines']
    start_date, end_date = _get_dates()
    return {**params, 'startDate': start_date, 'endDate': end_date}


async def _request_betting_lines(league: str, offer_id: str) -> None:
    url = payload['urls']['prop_lines'].format(offer_id)
    headers = payload['headers']
    params = _get_params()
    if resp_json := await fetch(url, headers=headers, params=params):
        _parse_betting_lines(league, resp_json)


def _extract_subject(event: dict) -> Optional[dict]:
    if (participants := event.get('participants')) and (first_participants := participants[0]):
        if raw_subject_name := first_participants.get('name'):
            return raw_subject_name

        raise ValueError(f'No subject name found in event participants: {event}')


def _extract_bookmaker(outcome: dict) -> Optional[str]:
    if bookmaker := outcome.get('sportsbookCode'):
        return bookmaker


def _extract_odds(outcome: dict) -> Optional[float]:
    if odds := outcome.get('odds'):
        return round(odds, 2)


def _extract_tw_prb(data: dict) -> Optional[float]:
    if tw_prob := data.get('trueWinProbability'):
        return round(tw_prob, 3)


def extract_ev(data: dict) -> Optional[float]:
    if ev := data.get('ev'):
        return round(ev, 3)


def _parse_betting_lines(league: str, resp: dict) -> None:
    global betting_lines
    for event in resp:
        if market := event.get('offerName'):
            if subject := _extract_subject(event):
                for side in event.get('sides', []):
                    if label := side.get('label'):
                        for outcome in side.get('outcomes', []):
                            if bookmaker_name := _extract_bookmaker(outcome):
                                if odds := _extract_odds(outcome):
                                    betting_lines.append({
                                        'bookmaker': bookmaker_name,
                                        'league': league,
                                        'market': market,
                                        'subject': subject,
                                        'label': label,
                                        'line': float(outcome.get('line', 0.5)),
                                        'odds': odds,
                                        'impl_prb': 1 / odds
                                    })

async def run_oddsshopper_scraper():
    try:
        # Todo: modify the process above and below so that this method acts more as a controller
        await _request_matchups()
        return pd.DataFrame(betting_lines)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    asyncio.run(run_oddsshopper_scraper())
