from datetime import datetime
from typing import Optional, Iterable, Union

from app.cache import main
from app.services.betting_lines.data_collection.utils import post, fetch, get_payload


PAYLOAD = get_payload('BoomFantasy')


def _get_tokens(token_type: str = None) -> Optional[Union[str, dict]]:
    credentials = PAYLOAD['json_data']['tokens']['authentication']['credentials']
    return credentials.get(token_type, credentials)


def _update_tokens(tokens: dict) -> None:
    PAYLOAD['json_data']['tokens']['authentication']['credentials'].update(tokens)
    headers = PAYLOAD['headers']
    headers['prop_lines']['authorization'] = f'Bearer {tokens["accessToken"]}'
    headers['contest_ids']['authorization'] = f'Bearer {tokens["accessToken"]}'


async def _request_new_tokens() -> bool:
    url = PAYLOAD['urls'].get('tokens')
    headers = PAYLOAD['headers'].get('tokens')
    json_data = PAYLOAD['json_data'].get('tokens')
    if resp_json := await post(url, headers=headers, json=json_data):
        relevant_tokens = {k: v for k, v in resp_json.items() if k in ['accessToken', 'refreshToken']}
        _update_tokens(relevant_tokens)
        return True

    return False


async def _request_contest_id() -> Optional[str]:
    url = PAYLOAD['urls'].get('contest_ids')
    headers = PAYLOAD['headers'].get('contest_ids')
    json_data = PAYLOAD['json_data'].get('contest_ids')
    if resp_json := await post(url, headers=headers, json=json_data):
        return _parse_contest_id(resp_json)


def _parse_contest_id(resp: dict) -> Optional[str]:
    if data := resp.get('data'):
        for contest in data.get('contests', []):
            if contest.get('title') == "Pick' Em":
                return contest.get('_id')


async def _request_betting_lines(contest_id: str) -> dict | None:
    url = PAYLOAD['urls']['prop_lines'].format(contest_id)
    headers = PAYLOAD['headers'].get('prop_lines')
    params = PAYLOAD['params'].get('prop_lines')
    if resp_json := await fetch(url, headers=headers, params=params):
        return resp_json


def _get_sections(resp: dict) -> Iterable:
    if data := resp.get('data'):
        if contest := data.get('multiLineContest'):
            for section in contest.get('sections', []):
                yield section


def _extract_league(section: dict) -> Optional[str]:
    if raw_league_name := section.get('league'):
        cleaned_league_name = raw_league_name.strip().upper()
        return cleaned_league_name


def _get_qgs(section: dict) -> Iterable:
    if section.get('status') == 'active':
        for qg in section.get('qG', []):
            yield qg


def _get_qg_data(section: dict) -> Iterable:
    for qg in _get_qgs(section):
        yield qg


def _extract_team(qg: dict) -> Optional[str]:
    if player_image := qg.get('playerImage'):
        if team_name := player_image.get('abbreviation'):
            return team_name


def _extract_subject(qg: dict) -> Optional[str]:
    if title := qg.get('title'):
        if options := title.get('o'):
            if (raw_first_name := options.get('firstName')) and (raw_last_name := options.get('lastName')):
                raw_full_subj_name = ' '.join([raw_first_name, raw_last_name])
                return raw_full_subj_name


def _extract_period(qg: dict) -> Optional[str]:
    if (period := qg.get('periodClassifier')) and (period != 'fullGame'):
        return period


def _get_q_data(qg: dict) -> Iterable:
    yield _extract_subject(qg)
    for q in qg.get('q', []):
        yield q


def _extract_market(qg: dict, q: dict) -> Optional[str]:
    # Todo: handle when there is a period
    period = _extract_period(qg)
    if raw_market_name := q.get("statistic"):
        return raw_market_name


def _get_c_data(qg: dict, q: dict) -> Iterable:
    yield _extract_market(qg, q)
    if c_data := q.get('c', []):
        for c in c_data:
            yield c


def _get_c_data_2(c: dict) -> Iterable:
    yield c.get('l')
    for c_2_data in c.get('c', []):
        yield c_2_data


def _extract_label_and_odds(c_2: list) -> Optional[tuple[str, float]]:
    if len(c_2) == 3:
        if (label := c_2[1]) and (odds := c_2[2]):
            return label.title(), float(odds)


def _parse_betting_lines(resp: dict, collected_betting_lines: list[dict]) -> None:
    for section in _get_sections(resp):
        if league := _extract_league(section):
            for qg in _get_qg_data(section):
               q_data_iter = iter(_get_q_data(qg))
               if subject := next(q_data_iter):
                    for q in q_data_iter:
                        c_data_iter = iter(_get_c_data(qg, q))
                        if market := next(c_data_iter):
                            for c in c_data_iter:
                                c_2_data_iter = iter(_get_c_data_2(c))
                                if line := next(c_2_data_iter):
                                    for c_2 in c_2_data_iter:
                                        label, odds = _extract_label_and_odds(c_2)
                                        if label and odds:
                                            collected_betting_lines.append({
                                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                'bookmaker': 'BoomFantasy',
                                                'league': league,
                                                'subject': subject,
                                                'market': market,
                                                'label': label,
                                                'line': line,
                                                'odds': odds,
                                                'impl_prb': round(1 / odds, 4)
                                            })



async def run_boomfantasy_collector(collected_betting_lines: list) -> None:
    # Todo: don't get new tokens every time...randomize every three requests
    try:
        print('[BoomFantasy]: Running collector...')
        print('[BoomFantasy]: Requesting new tokens...')
        if await _request_new_tokens():
            print('[BoomFantasy]: Tokens received...')
            print('[BoomFantasy]: Requesting contest id...')
            if contest_id := await _request_contest_id():
                print('[BoomFantasy]: Contest id received...')
                print('[BoomFantasy]: Requesting prop lines...')
                if betting_lines_resp := await _request_betting_lines(contest_id):
                    print('[BoomFantasy]: Prop lines received...')
                    print('[BoomFantasy]: Parsing prop lines...')
                    _parse_betting_lines(betting_lines_resp, collected_betting_lines)
                    print('[BoomFantasy]: Prop lines parsed...')

    except Exception as e:
        raise e

if __name__ == '__main__':
    import asyncio
    session.client.delete('lines:info')
    asyncio.run(run_boomfantasy_collector([]))
