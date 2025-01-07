import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional, Iterable

from app.services.betting_lines.data_collection import utils
from app.services.betting_lines.data_collection.configs import CONFIGS


PAYLOAD = utils.requester.get_payload('OddsShopper')

num_betting_lines_collected = 0


async def _request_matchups() -> dict | None:
    url = PAYLOAD['urls']['matchups']
    headers = PAYLOAD['headers']
    cookies = PAYLOAD['cookies']
    if resp_json := await utils.requester.fetch(url, headers=headers, cookies=cookies):
        return resp_json


def _get_offers(resp: dict) -> Iterable:
    for offer_category in resp.get('offerCategories', []):
        if offer_category.get('name') == 'PlayerProps':
            for offer in offer_category.get('offers', []):
                yield offer


def _extract_league(offer: dict) -> str | None:
    if raw_league_name := offer.get('leagueCode'):
        cleaned_league_name = raw_league_name.strip().upper()
        if cleaned_league_name in CONFIGS['leagues_to_collect_from']:
            return cleaned_league_name


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
    if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
        _parse_betting_lines(league, resp_json, collected_betting_lines)


def _extract_market(event: dict, league: str) -> str | None:
    try:
        if raw_market_name := event.get('offerName'):
            sport = utils.standardizer.get_sport(league)
            std_market_name = utils.standardizer.standardize_market_name(raw_market_name, sport)
            return std_market_name

    except Exception as e:
        print('[OddsShopper]: !! ERROR -', e, '!!')


def _extract_subject(event: dict) -> Optional[dict]:
    try:
        if (participants := event.get('participants')) and (first_participants := participants[0]):
            if raw_subject_name := first_participants.get('name'):
                return raw_subject_name

            raise ValueError(f"No subject name found in event participants: '{event}'")

    except Exception as e:
        print('[OddsShopper]: !! ERROR -', e, '!!')


def _extract_bookmaker(outcome: dict) -> str | None:
    if bookmaker := outcome.get('sportsbookCode'):
        return bookmaker


def _extract_odds(outcome: dict) -> Optional[float]:
    if odds := outcome.get('odds'):
        return odds


def _extract_tw_prb(data: dict) -> Optional[float]:
    if tw_prob := data.get('trueWinProbability'):
        return tw_prob


def extract_ev(data: dict) -> Optional[float]:
    if ev := data.get('ev'):
        return ev


def _parse_betting_lines(league: str, resp: dict, collected_betting_lines: list[dict]) -> None:
    global num_betting_lines_collected
    for event in resp:
        if market := _extract_market(event, league):
            if subject := _extract_subject(event):
                for side in event.get('sides', []):
                    if label := side.get('label'):
                        for outcome in side.get('outcomes', []):
                            if bookmaker_name := _extract_bookmaker(outcome):
                                if odds := _extract_odds(outcome):
                                    betting_line_doc = {
                                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'bookmaker': bookmaker_name,
                                        'league': league,
                                        'market': market,
                                        'subject': subject,
                                        'label': label,
                                        'line': float(outcome.get('line', 0.5)),
                                        'odds': odds,
                                    }
                                    betting_line_doc_key = utils.get_betting_line_key(betting_line_doc)
                                    betting_line_unique_id = utils.generate_unique_id(betting_line_doc_key)
                                    betting_line_doc['_id'] = betting_line_unique_id
                                    collected_betting_lines.append(betting_line_doc)
                                    num_betting_lines_collected += 1


async def run_oddsshopper_collector(collected_betting_lines: list) -> None:
    global num_betting_lines_collected
    try:
        start_time = time.time()
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
            print(f'[Oddsshopper]: Collected {num_betting_lines_collected} betting lines...')
            num_betting_lines_collected = 0
            end_time = time.time()
            print(f'[Oddsshopper]: Time taken: {round(end_time - start_time, 2)} seconds')

    except Exception as e:
        print(e)


if __name__ == '__main__':
    asyncio.run(run_oddsshopper_collector([]))
