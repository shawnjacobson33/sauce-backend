from datetime import datetime
from typing import Optional, Iterable

from urllib3.exceptions import ResponseError

from app.services.utils import utilities as utils, Standardizer
from app.services.betting_lines.data_collection.base import BaseBettingLinesCollector


class BoomFantasyCollector(BaseBettingLinesCollector):
    
    def __init__(self,
                 batch_timestamp: datetime,
                 betting_lines_container: list[dict],
                 standardizer: Standardizer,
                 configs: dict):

        super().__init__('BoomFantasy', batch_timestamp, betting_lines_container, standardizer, configs)
    
    def _get_tokens(self, token_type: str = None) -> str | dict | None:
        credentials = self.payload['json_data']['tokens']['authentication']['credentials']
        return credentials.get(token_type, credentials)

    def _update_tokens(self, tokens: dict) -> None:
        self.payload['json_data']['tokens']['authentication']['credentials'].update(tokens)
        headers = self.payload['headers']
        headers['betting_lines']['authorization'] = f'Bearer {tokens["accessToken"]}'
        headers['contest_ids']['authorization'] = f'Bearer {tokens["accessToken"]}'

    async def _request_new_tokens(self) -> bool:
        try:
            url = self.payload['urls'].get('tokens')
            headers = self.payload['headers'].get('tokens')
            json_data = self.payload['json_data'].get('tokens')
            if resp_json := await utils.requester.post(url, headers=headers, json=json_data):
                relevant_tokens = {k: v for k, v in resp_json.items() if k in ['accessToken', 'refreshToken']}
                self._update_tokens(relevant_tokens)
                self.successful_requests += 1
                return True
        
        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1
            return False

    async def _request_contest_id(self) -> str | None:
        try:
            url = self.payload['urls'].get('contest_ids')
            headers = self.payload['headers'].get('contest_ids')
            json_data = self.payload['json_data'].get('contest_ids')
            if resp_json := await utils.requester.post(url, headers=headers, json=json_data):
                self.successful_requests += 1
                return self._parse_contest_id(resp_json)

        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1

    @staticmethod
    def _parse_contest_id(resp: dict) -> str | None:
        if data := resp.get('data'):
            for contest in data.get('contests', []):
                if contest.get('title') == "Pick' Em":
                    return contest.get('_id')

    async def _request_betting_lines(self, contest_id: str) -> dict | None:
        try:
            url = self.payload['urls']['betting_lines'].format(contest_id)
            headers = self.payload['headers'].get('betting_lines')
            params = self.payload['params'].get('betting_lines')
            if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
                self.successful_requests += 1
                return resp_json

        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1

    @staticmethod
    def _get_sections(resp: dict) -> Iterable:
        if data := resp.get('data'):
            if contest := data.get('multiLineContest'):
                for section in contest.get('sections', []):
                    yield section

    def _extract_league(self, section: dict) -> str | None:
        if raw_league_name := section.get('league'):
            cleaned_league_name = raw_league_name.strip().upper()
            if cleaned_league_name in self.configs['valid_leagues']:
                return cleaned_league_name
    
    @staticmethod
    def _get_qgs(section: dict) -> Iterable:
        if section.get('status') == 'active':
            for qg in section.get('qG', []):
                yield qg

    def _get_qg_data(self, section: dict) -> Iterable:
        for qg in self._get_qgs(section):
            yield qg
    
    @staticmethod
    def _extract_team(qg: dict) -> str | None:
        if player_image := qg.get('playerImage'):
            if team_name := player_image.get('abbreviation'):
                return team_name

    @staticmethod
    def _get_raw_subject_name(qg: dict):
        if title := qg.get('title'):
            if options := title.get('o'):
                if (raw_first_name := options.get('firstName')) and (raw_last_name := options.get('lastName')):
                    return ' '.join([raw_first_name, raw_last_name])

    def _extract_subject(self, qg: dict, league: str) -> str | None:
        try:
            if raw_subject_name := self._get_raw_subject_name(qg):
                cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                return std_subject_name
    
        except Exception as e:
            self.log_error(e)
            self.failed_subject_standardization += 1

    def _extract_period(self, qg: dict) -> str | None:
        if (period := qg.get('periodClassifier')) and (period != 'fullGame'):
            std_period_name = self.standardizer.standardize_period_name(period)
            return std_period_name

    def _get_q_data(self, qg: dict, league: str) -> Iterable:
        yield self._extract_subject(qg, league)
        for q in qg.get('q', []):
            yield q

    def _extract_market(self, qg: dict, q: dict, league: str) -> str | None:
        try:
            period = self._extract_period(qg)
            if raw_market_name := q.get("statistic"):
                sport = utils.get_sport(league)
                std_market_name = self.standardizer.standardize_market_name(raw_market_name, sport, period=period)
                return std_market_name
    
        except ValueError as e:
            self.log_error(e)
            self.failed_market_standardization += 1

    def _get_c_data(self, qg: dict, q: dict, league: str) -> Iterable:
        yield self._extract_market(qg, q, league)
        if c_data := q.get('c', []):
            for c in c_data:
                yield c
    
    @staticmethod
    def _get_c_data_2(c: dict) -> Iterable:
        yield c.get('l')
        for c_2_data in c.get('c', []):
            yield c_2_data
    
    @staticmethod
    def _extract_label_and_odds(c_2: list) -> Optional[tuple[str, float]]:
        if len(c_2) == 3:
            if (label := c_2[1]) and (odds := c_2[2]):
                return label.title(), float(odds)

    async def _parse_betting_lines(self, resp: dict) -> None:
        for section in self._get_sections(resp):
            if league := self._extract_league(section):
                for qg in self._get_qg_data(section):
                   q_data_iter = iter(self._get_q_data(qg, league))
                   if subject := next(q_data_iter):
                       if game := await self._get_game(league, subject):
                           for q in q_data_iter:
                            c_data_iter = iter(self._get_c_data(qg, q, league))
                            if market := next(c_data_iter):
                                for c in c_data_iter:
                                    c_2_data_iter = iter(self._get_c_data_2(c))
                                    if line := next(c_2_data_iter):
                                        for c_2 in c_2_data_iter:
                                            label, odds = self._extract_label_and_odds(c_2)
                                            if label and odds:
                                                curr_datetime = datetime.now()
                                                betting_line_doc = {
                                                    'batch_timestamp': self.batch_timestamp,
                                                    'collection_timestamp': curr_datetime,
                                                    'date': datetime.strptime(curr_datetime.strftime('%Y-%m-%d'), '%Y-%m-%d'),
                                                    'bookmaker': self.name,
                                                    'league': league,
                                                    'game': game,
                                                    'market': market,
                                                    'subject': subject,
                                                    'label': label,
                                                    'line': line,
                                                    'odds': odds,
                                                }
                                                betting_line_doc_key = utils.storer.get_betting_line_key(betting_line_doc)
                                                betting_line_doc['_id'] = betting_line_doc_key
                                                self.items_container.append(betting_line_doc)
                                                self.num_collected += 1
    

    @utils.logger.collector_logger(message='Running Collector')
    async def run_collector(self) -> None:
        if await self._request_new_tokens():
            if contest_id := await self._request_contest_id():
                if betting_lines_resp := await self._request_betting_lines(contest_id):
                    await self._parse_betting_lines(betting_lines_resp)
