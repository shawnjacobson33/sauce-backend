from typing import Optional, Iterable, Union

from requests import Response

from app.cache import redis_cache as r


logistics = r.data_providers.getprovider('BoomFantasy')


def _get_url(url_type: str) -> Optional[str]:
    return logistics['urls'].get(url_type)


def _get_headers(headers_type: str) -> Optional[dict]:
    headers = logistics['headers'][headers_type]
    if headers_type == 'prop_lines':
        headers['Authorization'] = f'Bearer {_read_tokens("access_token")}'

    return headers

def _get_params() -> dict:
    return logistics['params']


def _get_json_data() -> dict:
    return logistics['json_data']


def _read_tokens(token_type: str = None) -> Optional[Union[str, dict]]:
    return logistics['tokens'].get(token_type) if token_type else logistics['tokens']


async def _request_prop_lines() -> None:
    url = _get_url('prop_lines')
    headers = _get_headers('prop_lines')
    params = _get_params()



def _get_sections(response: Response) -> Iterable:
    if (json_data := response.json()) and (data := json_data.get('data')):
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


def _get_qg_data(response: Response) -> Iterable:
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


def _get_qs_data(league: str, qg: dict) -> Iterable:
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


def _get_cs_data(league: str, qg: dict, q: dict) -> Iterable:
    yield _extract_market(league, qg, q)
    if c_data := q.get('c', []):
        for c in c_data:
            yield c


def _get_cs_data_2(c: dict) -> Iterable:
    yield c.get('l')
    for c_2_data in c.get('c', []):
        yield c_2_data


def _extract_label_and_odds(c_2: list) -> Optional[tuple[str, float]]:
    if len(c_2) == 3:
        if (label := c_2[1]) and (odds := c_2[2]):
            return label.title(), float(odds)


def _parse_prop_lines(response: Response) -> None:
    qg_data_iter = iter(_get_qg_data(response))
    if league := next(qg_data_iter):
        for qg in qg_data_iter:
           qs_data_iter = iter(_get_qs_data(league, qg))
           if game := next(qs_data_iter):
               if subject := next(qs_data_iter):
                    for q in qs_data_iter:
                        cs_data_iter = iter(_get_cs_data(league, qg, q))
                        if market := next(cs_data_iter):
                            for c in cs_data_iter:
                                cs_2_data_iter = iter(_get_cs_data_2(c))
                                if line := next(cs_2_data_iter):
                                    for c_2 in cs_2_data_iter:
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
                                                'implied_probability': round(1 / odds, 4)
                                            }


def _store_prop_lines():
    r.lines.storelines(func=_parse_prop_lines)


def _request_new_tokens() -> None:
    pass


def _parse_new_tokens() -> tuple[str, str]:
    pass





def extract_team(bookmaker_name: str, league: str, data: dict) -> Optional[tuple[str, str]]:
    # get a dictionary, if exists keep going
    if player_image_data := data.get('playerImage'):
        # get the team name if it exists
        if team_name := player_image_data.get('abbreviation'):
            # get the team id and team name from the database
            return dc_utils.get_team(bookmaker_name, league, team_name)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: str) -> Optional[dict[str, str]]:
    # gets the league section's title and options from that title, executes if they both exist
    if (title := data.get('title')) and (options := title.get('o')):
        # gets the first and last name of the player, executes if both exist
        if (first_name := options.get('firstName')) and (last_name := options.get('lastName')):
            # get subject name
            subject_name = ' '.join([first_name, last_name])
            # DATA LOOKS LIKE --> {'id': 123asd, 'name': 'Jayson Tatum'} POSSIBLY WITH 'team': 'BOS'
            return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


def extract_line(data: dict) -> Optional[tuple[str, str]]:
    # get the pick selection data, execute if exists
    if pick_selection_title := data.get('pickSelectionTitle'):
        # get additional data from pick selection, execute if exists
        if additional_options := pick_selection_title.get('additionalOptions'):
            # get market data and get numeric over/under line, execute if both exist
            if (stat_text := additional_options.get('statText')) and (line := additional_options.get('line')):
                # return the numeric line for the prop line and some additional market data
                return line, stat_text


def extract_period(data: dict) -> Optional[str]:
    # gets the period (fullGame, firstQuarter, etc.), keep executing if it exists and if it isn't fullGame
    if (period := data.get('periodClassifier')) and (period != 'fullGame'):
        # return the period classifier
        return period


def extract_market(bookmaker_name: str, data: dict, league: str, period_type: Optional[str] = None) -> Optional[dict[str, str]]:
    # get the market name, if exists keep going
    if market_name := data.get("statistic"):
        # gets the market id
        market = dc_utils.get_market(bookmaker_name, league, market_name, period_type=period_type)
        # return both market id and cleaned market
        return market


def extract_label_and_odds(data: list) -> Optional[tuple[str, float]]:
    # check if the data list has the expected number of elements
    if len(data) == 3:
        # return a capitalized label and the odds
        return data[1].title(), float(data[2])


class BoomFantasy(ln_utils.LinesRetriever):
    def __init__(self, batch_id: str, bookmaker: ln_utils.LinesSource):
        super().__init__(batch_id, bookmaker)

    async def retrieve(self) -> None:
        url = ln_utils.get_url(self.name)
        headers = ln_utils.get_headers(self.name)
        params = ln_utils.get_params(self.name)
        tokens_data = {
            'url': ln_utils.get_url(self.name, name='tokens'),
            'headers': ln_utils.get_headers(self.name, name='tokens'),
            'json_data': ln_utils.get_json_data(self.name, name='tokens')
        }
        await self.req_mngr.get_bf(url, tokens_data, self._parse_lines, headers=headers, params=params)

    async def _parse_lines(self, response) -> None:
        if (json_data := response.json()) and (data := json_data.get('data')):
            if contest := data.get('multiLineContest'):
                for section_data in contest.get('sections', []):
                    if section_data.get('status') == 'active':
                        if league := extract_league(self.name, section_data):
                            sport = dc_utils.LEAGUE_SPORT_MAP[league]
                            for qg_data in section_data.get('qG', []):
                                if team_id := extract_team(self.name, league, qg_data):
                                    if game := dc_utils.get_game(team_id):
                                        if subject := extract_subject(self.name, qg_data, league, team_id[1]):
                                            period = extract_period(qg_data)
                                            for q_data in qg_data.get('q', []):
                                                if market := extract_market(self.name, q_data, league, period):
                                                    for c_data in q_data.get('c', []):
                                                        if line := c_data.get('l'):
                                                            for more_c_data in c_data.get('c', []):
                                                                label, odds = extract_label_and_odds(more_c_data)
                                                                if label and odds:
                                                                    self.store({
                                                                        'bookmaker': self.name,
                                                                        'sport': sport,
                                                                        'league': league,
                                                                        'game_time': game['game_time'],
                                                                        'game': game['info'],
                                                                        'market': market['name'],
                                                                        'subject_id': subject['id'],
                                                                        'subject': subject['name'],
                                                                        'label': label,
                                                                        'line': line,
                                                                        'odds': odds,
                                                                        'im_prb': round(1 / odds, 4)
                                                                    })
