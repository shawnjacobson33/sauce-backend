import json
import sys
from typing import Optional, Iterable, Union

from app.cache import redis_cache as r
from app.tasks.lines.utils import post, fetch

# r.data_providers.storeproviders([
#     {
#         'name': 'BoomFantasy',
#         'urls': {
#             'contest_id': 'https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/graphql',
#             'tokens': 'https://production-api.boomfantasy.com/api/v1/sessions',
#             'prop_lines': 'https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/contests/multiLine/{}'
#         },
#         'headers': {
#             'prop_lines': {
#                 'Host': 'production-boom-dfs-backend-api.boomfantasy.com',
#                 'access-control-allow-origin': '*',
#                 'accept': 'application/json, text/plain, */*',
#                 'x-product-id': 'boom_dfs',
#                 'authorization': 'Bearer {}',
#                 'x-app-name': 'Boom',
#                 'accept-language': 'en-US,en;q=0.9',
#                 'user-agent': 'BoomDFS/3 CFNetwork/1568.100.1.2.1 Darwin/24.0.0',
#                 'x-device-id': 'D03577BA-B845-4E42-ADE3-59BB344E4AA9',
#                 'x-app-build': '3',
#                 'x-platform': 'ios',
#             },
#             'contest_id': {
#                 'Host': 'production-boom-dfs-backend-api.boomfantasy.com',
#                 'content-type': 'application/json',
#                 'access-control-allow-origin': '*',
#                 'accept': 'application/json, text/plain, */*',
#                 'x-product-id': 'boom_dfs',
#                 'authorization': 'Bearer {}',
#                 'x-app-name': 'Boom',
#                 'accept-language': 'en-US,en;q=0.9',
#                 'user-agent': 'BoomDFS/1 CFNetwork/1568.200.51 Darwin/24.1.0',
#                 'x-device-id': 'D03577BA-B845-4E42-ADE3-59BB344E4AA9',
#                 'x-app-build': '1',
#                 'x-app-version': '32.2',
#                 'x-platform': 'ios',
#             },
#             'tokens': {
#                 'Host': 'production-api.boomfantasy.com',
#                 'content-type': 'application/json',
#                 'accept': 'application/json',
#                 'x-product-id': 'boom_dfs',
#                 'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXIiOjIsInR5cCI6ImFub24iLCJ0aWQiOiJib29tLWRmcyIsInBpZCI6ImJvb21fZGZzIiwiaWF0IjoxNjYxMjgyMjg3fQ.Cty9gHSp0dDdNR5iy1csmb98iSFdebBupQ4kRnGblI5irfdWlQScZ-BGEiHmUD7CCL3g8N5ji2vKkwAc40tQAHY0WDHpbCEJ9ebLZ0r5HfQeeBN9HjdYj9aCjkx_gtWbHBKQuoJ5zllZR2JG69G-ptxgtw8dvffiyfjphXOGdg7am6WnxcYNx4GiJkdrN1_IEcrKqyDNFmyQVV2hg1xDwa_Al7_YQZosYtgMU9rWMAGM3gz3nB2WlP_Fv9ZcJTvp65ZBSutSRxVWHu-sQze8WLh6VyTzHaUzGStrsTmiv0_i8fhoABerZvg7srkUp17ITwPWSd2LTcS--mhpI64IhSiF-Hqq_9yk6mGX7cs9c-dkiO0aSWrdtIrkscr6HgxtwaH8HQCOpRBfPq0ev_PABpnbGLy7lxJ6G3LPtq2si_vJyOnLfXX08qL93OexfTm-QYIglUAuoVJPRZaoRVhcTlw5Jkbp96HY763gRpmLhhp41IClJQI75UKXgTi937m-ZgSL2VE6ypd4xSkrl46LSchkzdf3jh3ArELsTGws9Fi_eY1-_ivcbJaZdM_tw0QE-sId1kyx2noQvyW8C4ETeAfmy_G3xkn_6tECV1dZ4ppMKzkXMr7o1dIOLraYYdwzDXRWzoyDQ4kujnGzGPoROSlzp3fdeoRpyEPcsiIbxwc',
#                 'x-app-name': 'Boom',
#                 'accept-language': 'en-US,en;q=0.9',
#                 'user-agent': 'BoomDFS/1 CFNetwork/1568.200.51 Darwin/24.1.0',
#                 'x-device-id': 'D03577BA-B845-4E42-ADE3-59BB344E4AA9',
#                 'x-app-build': '1',
#                 'x-app-version': '32.2',
#                 'x-platform': 'ios',
#             }
#         },
#         'params': {
#             'prop_lines': {
#                 'questionStatus': 'available',
#             }
#         },
#         'json_data': {
#             'contest_id': {
#                 "query": "\n        query GetLobbyData(\n            $contestStatuses: [ContestStatus!]!\n            $imageType: ContestLobbyImageType!\n            $userId: ID\n        ) {\n            contests(statuses: $contestStatuses) {\n                _id\n                title\n                status\n                type\n                renderType\n                lobby(imageType: $imageType) {\n                    title {\n                        i18nKey\n                        additionalOptions\n                    }\n                    image {\n                        type\n                        path\n                        source\n                    }\n                }\n            }\n            lobbyHeroes(contestStatuses: $contestStatuses) {\n                type\n                status\n                priority\n                action\n                isUrlExternal\n                url\n                image {\n                    path\n                    source\n                    type\n                }\n                button {\n                    marginTop\n                    width\n                    height\n                }\n                contest {\n                    _id\n                    title\n                    type\n                    # navigation props\n                    renderType\n                    section\n                    league\n                    periodClassifier\n                    statistic\n                }\n            }\n            depositCTA {\n                image {\n                    type\n                    source\n                    path\n                }\n            }\n            depositHistory(userId: $userId) {\n                hasDeposited\n            }\n        }\n    ",
#                 "variables": {
#                     "contestStatuses": ["active", "upcoming"],
#                     "userId": "f69bbfae-6677-4075-a011-0fa48625da67",
#                     "imageType": "wide"
#                 },
#                 "operationName": "GetLobbyData"
#             },
#             'tokens': {
#                 'authentication': {
#                     'type': 'refresh',
#                     'credentials': {
#                         'refreshToken': 'f2349693266bff556fdb27180920733f438a73654e6e134696d75f53edb5c3c2a174de02fac6cebd7a0780aadf8a4b67b0979811ffb415aa400a850733a8a457',
#                         'accessToken': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXIiOjMsInR5cCI6ImF1dGgiLCJ1aWQiOiJmNjliYmZhZS02Njc3LTQwNzUtYTAxMS0wZmE0ODYyNWRhNjciLCJzaWQiOiJkMjM1OTQyNy0zZjU1LTRkNGEtOTQ3ZC0xOTdlODc4ODZlYTEiLCJ0aWQiOiJib29tLWRmcyIsInBpZCI6ImJvb21fZGZzIiwicm9sZXMiOlsxMDMsMTA0XSwiY29ob3J0Ijo0MCwiZXhwIjoxNzM1NjYwMTg3LCJpYXQiOjE3MzU2NTk1ODd9.RdWtfjnoZx2kKvgHM85aYsxOnwf2GdTk3DPZu9fQU3UOoJaF-J8FI5SgmloWOE3C8OemOlSJGq0eLoyYN5egap9tlJSEcw8jXz6pmTNdJWFpfQq7aI3Llc8HdzzivjZgCoRWSdM3p8lc-R2khXT0u0-aeRVR12QX2yvA56vDImwY-wKNb0ZNTWm1ScIKB-BCcon9qR-EpK6z17Au_QQ4M2YcwU386du45SqOhPqk7SXH_luLNRsygWTWVFrBXzFv4bWihlvva62n_H8TGM9f22Lx2ZAh-UYmoEDjUZO7ygCyQdzKLbZMzl9lrm5XhfYUkmdYqow4JUb_2B5Q1Udq-KeZfd7TtdtA7eQc6wl7wRrLXCGeauNQxUth-nHCF-CdrsD3yyhMBDy3zp_6XQMejVRDkpu4xB61vO_LAKZc_9DtfWWNkoxTrj2R6tlblpUKue7ccRI1iN2Fsx41NgrCE59o1576zwzQOR3D7fwWVTgUC8Zc_LANhLPwWEmyrw9GAYlt82iUe_wXMwNci9QBqTVhQJNaF9wq6jPgWssNqDUsAaZY6z_aO772JnKOPKnEC2wrpu57gfHphKs-Q-Iq-2P-v5HihoQvk6IASf7Mn_b7VlLSvtx0jNJ7P1M4ASQV9Voc0j0qXd4W9UGee6-Omf1ceNK_lZs0l_JUcIGQKCc',
#                     },
#                 },
#                 'eventInfo': {},
#             }
#         }
#     }
# ])


logistics = r.data_providers.getprovider('BoomFantasy')


def _get_tokens(token_type: str = None) -> Optional[Union[str, dict]]:
    credentials = logistics['json_data']['tokens']['authentication']['credentials']
    return credentials.get(token_type, credentials)


def _update_tokens(tokens: dict) -> None:
    logistics['json_data']['tokens']['authentication']['credentials'].update(tokens)
    headers = logistics['headers']
    headers['prop_lines']['authorization'] = f'Bearer {tokens["accessToken"]}'
    headers['contest_id']['authorization'] = f'Bearer {tokens["accessToken"]}'


async def _request_new_tokens() -> bool:
    url = logistics['urls'].get('tokens')
    headers = logistics['headers'].get('tokens')
    json_data = logistics['json_data'].get('tokens')
    if resp_json := await post(url, headers=headers, json=json_data):
        relevant_tokens = {k: v for k, v in resp_json.items() if k in ['accessToken', 'refreshToken']}
        _update_tokens(relevant_tokens)
        return True

    return False


async def _request_contest_id() -> Optional[str]:
    url = logistics['urls'].get('contest_id')
    headers = logistics['headers'].get('contest_id')
    json_data = logistics['json_data'].get('contest_id')
    if resp_json := await post(url, headers=headers, json=json_data):
        return _parse_contest_id(resp_json)


def _parse_contest_id(resp: dict) -> Optional[str]:
    if data := resp.get('data'):
        for contest in data.get('contests', []):
            if contest.get('title') == "Pick' Em":
                return contest.get('_id')


async def _request_prop_lines(contest_id: str) -> Iterable:
    url = logistics['urls']['prop_lines'].format(contest_id)
    headers = logistics['headers'].get('prop_lines')
    params = logistics['params'].get('prop_lines')
    if resp_json := await fetch(url, headers=headers, params=params):
        print("PROP LINES RESPONSE MEMORY SIZE:", round(sys.getsizeof(resp_json) / 1024, 2), "KB")
        return _parse_prop_lines(resp_json)


def _get_sections(resp: dict) -> Iterable:
    if data := resp.get('data'):
        if contest := data.get('multiLineContest'):
            for section in contest.get('sections', []):
                yield section


def _extract_league(section: dict) -> Optional[str]:
    if raw_league_name := section.get('league'):
        cleaned_league_name = raw_league_name.strip().upper()
        return r.leagues.getleague(cleaned_league_name)


def _get_qgs(section: dict) -> Iterable:
    if section.get('status') == 'active':
        for qg in section.get('qG', []):
            yield qg


def _get_qg_data(response: dict) -> Iterable:
    for section in _get_sections(response):
        yield _extract_league(section)
        for qg in _get_qgs(section):
            yield qg


def _extract_team(league: str, qg: dict) -> Optional[str]:
    if player_image := qg.get('playerImage'):
        if team_name := player_image.get('abbreviation'):
            return r.teams.getid(league, team_name)


def _extract_subject(league: str, team: str, qg: dict) -> Optional[dict[str, str]]:
    if title := qg.get('title'):
        if options := title.get('o'):
            if (raw_first_name := options.get('firstName')) and (raw_last_name := options.get('lastName')):
                raw_full_subj_name = ' '.join([raw_first_name, raw_last_name])
                return r.subjects.getsubj(league, raw_full_subj_name, team)


def _extract_period(qg: dict) -> Optional[str]:
    if (period := qg.get('periodClassifier')) and (period != 'fullGame'):
        return period


def _get_q_data(league: str, qg: dict) -> Iterable:
    if team_name := _extract_team(league, qg):
        yield r.games.getgame(league, team_name)
        yield _extract_subject(league, team_name, qg)
        for q in qg.get('q', []):
            yield q


def _extract_market(sport: str, qg: dict, q: dict) -> Optional[dict[str, str]]:
    # Todo: handle when there is a period
    period = _extract_period(qg)
    if raw_market_name := q.get("statistic"):
        return r.markets.getmarket(sport, raw_market_name)


def _get_c_data(league: str, qg: dict, q: dict) -> Iterable:
    yield _extract_market(league, qg, q)
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


def _parse_prop_lines(resp: dict) -> Iterable:
    qg_data_iter = iter(_get_qg_data(resp))
    if league := next(qg_data_iter):
        for qg in qg_data_iter:
           q_data_iter = iter(_get_q_data(league, qg))
           if game := next(q_data_iter):
               if subject := next(q_data_iter):
                    for q in q_data_iter:
                        c_data_iter = iter(_get_c_data(league, qg, q))
                        if market := next(c_data_iter):
                            for c in c_data_iter:
                                c_2_data_iter = iter(_get_c_data_2(c))
                                if line := next(c_2_data_iter):
                                    for c_2 in c_2_data_iter:
                                        label, odds = _extract_label_and_odds(c_2)
                                        if label and odds:
                                            yield {
                                                'bookmaker': 'BoomFantasy',
                                                'league': league,
                                                'game': game,
                                                'subject': subject,
                                                'market': market,
                                                'label': label,
                                                'line': line,
                                                'odds': odds,
                                                'impl_prb': round(1 / odds, 4)
                                            }


async def run():
    # Todo: don't get new tokens every time...randomize every three requests
    try:
        if await _request_new_tokens():
            if contest_id := await _request_contest_id():
                if lines_parser := await _request_prop_lines(contest_id):
                    r.lines.storelines(lines_parser)

    except Exception as e:
        print(e)

    finally:
        r.client.hset('providers', logistics['name'], json.dumps(logistics))


if __name__ == '__main__':
    import asyncio
    asyncio.run(run())
