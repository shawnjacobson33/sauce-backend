import asyncio
from datetime import datetime, timedelta
from typing import Optional, Iterable

from app.cache import session
from app.services.betting_lines.data_collection.utils import fetch, get_payload


PAYLOAD = get_payload('OddsShopper')
IN_SEASON_AND_VALID_LEAGUES = { 'NBA', 'NFL', 'NHL' }


async def _request_matchups() -> dict | None:
    url = PAYLOAD['urls']['matchups']
    headers = PAYLOAD['headers']
    cookies = PAYLOAD['cookies']
    if resp_json := await fetch(url, headers=headers, cookies=cookies):
        return resp_json


def _get_offers(resp: dict) -> Iterable:
    for offer_category in resp.get('offerCategories', []):
        if offer_category.get('name') == 'PlayerProps':
            for offer in offer_category.get('offers', []):
                yield offer


def _extract_league(offer: dict) -> Optional[str]:
    if raw_league_name := offer.get('leagueCode'):
        if raw_league_name in IN_SEASON_AND_VALID_LEAGUES:
            return raw_league_name.strip().upper()


def _parse_matchups(resp: dict) -> tuple[str, str] | None:
    for offer in _get_offers(resp):
        if (league := _extract_league(offer)) and (offer_id := offer.get('id')):
            yield league, offer_id

def _get_dates() -> tuple[str, str]:
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    return (datetime.now().strftime(date_format)[:-3] + 'Z',
            (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z')


def _get_params():
    params = PAYLOAD['params']['prop_lines']
    start_date, end_date = _get_dates()
    return {**params, 'startDate': start_date, 'endDate': end_date}


async def _request_betting_lines(league: str, offer_id: str, collected_betting_lines: list[dict]) -> None:
    url = PAYLOAD['urls']['prop_lines'].format(offer_id)
    headers = PAYLOAD['headers']
    params = _get_params()
    if resp_json := await fetch(url, headers=headers, params=params):
        _parse_betting_lines(league, resp_json, collected_betting_lines)


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


def _parse_betting_lines(league: str, resp: dict, collected_betting_lines: list[dict]) -> None:
    for event in resp:
        if market := event.get('offerName'):
            if subject := _extract_subject(event):
                for side in event.get('sides', []):
                    if label := side.get('label'):
                        for outcome in side.get('outcomes', []):
                            if bookmaker_name := _extract_bookmaker(outcome):
                                if odds := _extract_odds(outcome):
                                    collected_betting_lines.append({
                                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'bookmaker': bookmaker_name,
                                        'league': league,
                                        'market': market,
                                        'subject': subject,
                                        'label': label,
                                        'line': float(outcome.get('line', 0.5)),
                                        'odds': odds,
                                        'impl_prb': 1 / odds
                                    })

async def run_oddsshopper_collector(collected_betting_lines: list) -> None:
    try:
        print('[Oddsshopper]: Running collector...')
        print('[Oddsshopper]: Requesting matchups...')
        if matchups_resp := await _request_matchups():
            print('[OddsShopper]: Matchups received...')
            betting_lines_tasks = []
            for event_data in _parse_matchups(matchups_resp):
                if event_data:
                    league, offer_id = event_data
                    betting_lines_tasks.append(_request_betting_lines(league, offer_id, collected_betting_lines))

            print('[Oddsshopper]: Requesting betting lines...')
            await asyncio.gather(*betting_lines_tasks)
            print('[Oddsshopper]: Betting lines received...')

    except Exception as e:
        print(e)


if __name__ == '__main__':
    session.client.delete('lines:info')
    asyncio.run(run_oddsshopper_collector())
