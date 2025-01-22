from datetime import datetime
from typing import Optional, Iterable

from pipelines.utils import utilities as utils
from pipelines.utils.standardization import Standardizer
from pipelines.utils.exceptions import StandardizationError, RequestingError

from pipelines.base.base_collector import collector_logger
from pipelines.betting_lines.data_collection.betting_lines_collector_base import BaseBettingLinesCollector


class BoomFantasyCollector(BaseBettingLinesCollector):
    """
    A class to collect betting lines data from BoomFantasy.

    Attributes:
        batch_timestamp (datetime): The timestamp of the batch.
        betting_lines_container (list[dict]): The container to store betting lines.
        standardizer (Standardizer): The standardizer for data.
        configs (dict): The configuration settings.
    """

    def __init__(self,
                 batch_timestamp: datetime,
                 betting_lines_container: list[dict],
                 standardizer: Standardizer,
                 configs: dict):
        """
        Initializes the BoomFantasyCollector with the given parameters.

        Args:
            batch_timestamp (datetime): The timestamp of the batch.
            betting_lines_container (list[dict]): The container to store betting lines.
            standardizer (Standardizer): The standardizer for data.
            configs (dict): The configuration settings.
        """
        super().__init__('BoomFantasy', batch_timestamp, betting_lines_container, standardizer, configs)

    def _get_tokens(self, token_type: str = None) -> str | dict | None:
        """
        Retrieves the tokens for authentication.

        Args:
            token_type (str, optional): The type of token to retrieve. Defaults to None.

        Returns:
            str | dict | None: The token(s) if found, otherwise None.
        """
        credentials = self.payload['json_data']['tokens']['authentication']['credentials']
        return credentials.get(token_type, credentials)

    def _update_tokens(self, tokens: dict) -> None:
        """
        Updates the tokens in the payload.

        Args:
            tokens (dict): The new tokens to update.
        """
        self.payload['json_data']['tokens']['authentication']['credentials'].update(tokens)
        headers = self.payload['headers']
        headers['betting_lines']['authorization'] = f'Bearer {tokens["accessToken"]}'
        headers['contest_ids']['authorization'] = f'Bearer {tokens["accessToken"]}'

    async def _request_new_tokens(self) -> bool:
        """
        Requests new tokens from the BoomFantasy API.

        Returns:
            bool: True if the request was successful, otherwise False.
        """
        try:
            url = self.payload['urls'].get('tokens')
            headers = self.payload['headers'].get('tokens')
            json_data = self.payload['json_data'].get('tokens')
            if resp_json := await utils.requester.post(url, headers=headers, json=json_data):
                relevant_tokens = {k: v for k, v in resp_json.items() if k in ['accessToken', 'refreshToken']}
                self._update_tokens(relevant_tokens)
                self.successful_requests += 1
                return True

        except RequestingError as e:
            self.log_message(e, level='ERROR')
            self.failed_requests += 1
            return False

        except Exception as e:
            self.log_message(e, level='EXCEPTION')
            return False

    async def _request_contest_id(self) -> str | None:
        """
        Requests the contest ID from the BoomFantasy API.

        Returns:
            str | None: The contest ID if found, otherwise None.
        """
        try:
            url = self.payload['urls'].get('contest_ids')
            headers = self.payload['headers'].get('contest_ids')
            json_data = self.payload['json_data'].get('contest_ids')
            if resp_json := await utils.requester.post(url, headers=headers, json=json_data):
                self.successful_requests += 1
                return self._parse_contest_id(resp_json)

        except RequestingError as e:
            self.log_message(e, level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _parse_contest_id(self, resp: dict) -> str | None:
        """
        Parses the contest ID from the response data.

        Args:
            resp (dict): The response data.

        Returns:
            str | None: The contest ID if found, otherwise None.
        """
        try:
            if (data := resp['data']) and (isinstance(data, dict)):
                for contest in data['contests']:
                    if contest['title'] == "Pick' Em":
                        return contest['_id']

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _get_payload_for_betting_lines(self, contest_id: str) -> tuple[str, dict, dict] | None:
        """
        Gets the payload for the betting lines request.

        Args:
            contest_id (str): The contest ID.

        Returns:
            tuple[str, dict, dict] | None: The URL, headers, and parameters for the request.
        """
        url = self.payload['urls']['betting_lines'].format(contest_id)
        headers = self.payload['headers']['betting_lines']
        params = self.payload['params']['betting_lines']
        return url, headers, params

    async def _request_betting_lines(self, contest_id: str) -> dict | None:
        """
        Requests betting lines data from the BoomFantasy API.

        Args:
            contest_id (str): The contest ID.

        Returns:
            dict | None: The response data if successful, otherwise None.
        """
        try:
            url, headers, params = self._get_payload_for_betting_lines(contest_id)
            if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
                self.successful_requests += 1
                return resp_json

        except RequestingError as e:
            self.log_message(e, level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _get_sections(self, resp: dict) -> Iterable:
        """
        Extracts sections from the response data.

        Args:
            resp (dict): The response data.

        Yields:
            dict: The section data.
        """
        try:
            if data := resp['data']:
                if contest := data['multiLineContest']:
                    for section in contest['sections']:
                        yield section

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_league(self, section: dict) -> str | None:
        """
        Extracts the league from the section data.

        Args:
            section (dict): The section data.

        Returns:
            str | None: The cleaned league name if valid, otherwise None.
        """
        try:
            if raw_league_name := section['league']:
                cleaned_league_name = raw_league_name.strip().upper()
                if cleaned_league_name in self.configs['valid_leagues']:
                    return cleaned_league_name

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    @staticmethod
    def _get_qgs(section: dict) -> Iterable:
        """
        Extracts QGs from the section data.

        Args:
            section (dict): The section data.

        Yields:
            dict: The QG data.
        """
        if section['status'] == 'active':
            for qg in section['qG']:
                yield qg

    def _get_qg_data(self, section: dict) -> Iterable:
        """
        Extracts QG data from the section.

        Args:
            section (dict): The section data.

        Yields:
            dict: The QG data.
        """
        try:
            for qg in self._get_qgs(section):
                yield qg

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    @staticmethod
    def _extract_team(qg: dict) -> str | None:
        """
        Extracts the team from the QG data.

        Args:
            qg (dict): The QG data.

        Returns:
            str | None: The team name if found, otherwise None.
        """
        player_image = qg['playerImage']
        team_name = player_image['abbreviation']
        return team_name

    @staticmethod
    def _get_raw_subject_name(qg: dict):
        """
        Extracts the raw subject name from the QG data.

        Args:
            qg (dict): The QG data.

        Returns:
            str: The raw subject name.
        """
        if title := qg.get('title'):
            if options := title.get('o'):
                if (raw_first_name := options.get('firstName')) and (raw_last_name := options.get('lastName')):
                    return ' '.join([raw_first_name, raw_last_name])

    def _extract_subject(self, qg: dict, league: str) -> str | None:
        """
        Extracts the subject from the QG data.

        Args:
            qg (dict): The QG data.
            league (str): The league name.

        Returns:
            str | None: The standardized subject name if valid, otherwise None.
        """
        try:
            if raw_subject_name := self._get_raw_subject_name(qg):
                cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                return std_subject_name

        except Exception as e:
            self.log_message(e, level='EXCEPTION')
            self.failed_subject_standardization += 1

    def _extract_period(self, qg: dict) -> str | None:
        """
        Extracts the period from the QG data.

        Args:
            qg (dict): The QG data.

        Returns:
            str | None: The standardized period name if valid, otherwise None.
        """
        if (period := qg.get('periodClassifier')) and (period != 'fullGame'):
            std_period_name = self.standardizer.standardize_period_name(period)
            return std_period_name

    def _get_q_data(self, qg: dict, league: str) -> Iterable:
        """
        Extracts Q data from the QG data.

        Args:
            qg (dict): The QG data.
            league (str): The league name.

        Yields:
            dict: The Q data.
        """
        try:
            yield self._extract_subject(qg, league)
            for q in qg['q']:
                yield q

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_market(self, qg: dict, q: dict, league: str) -> str | None:
        """
        Extracts the market from the QG and Q data.

        Args:
            qg (dict): The QG data.
            q (dict): The Q data.
            league (str): The league name.

        Returns:
            str | None: The standardized market name if valid, otherwise None.
        """
        try:
            period = self._extract_period(qg)
            if raw_market_name := q.get("statistic"):
                sport = utils.get_sport(league)
                std_market_name = self.standardizer.standardize_market_name(
                    raw_market_name, 'PlayerProps', sport, period=period)
                return std_market_name

        except ValueError as e:
            self.log_message(e, level='EXCEPTION')
            self.failed_market_standardization += 1

    def _get_c_data(self, qg: dict, q: dict, league: str) -> Iterable:
        """
        Extracts C data from the QG and Q data.

        Args:
            qg (dict): The QG data.
            q (dict): The Q data.
            league (str): The league name.

        Yields:
            dict: The C data.
        """
        try:
            yield self._extract_market(qg, q, league)
            if c_data := q['c']:
                for c in c_data:
                    yield c

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _get_c_data_2(self, c: dict) -> Iterable:
        """
        Extracts C2 data from the C data.

        Args:
            c (dict): The C data.

        Yields:
            dict: The C2 data.
        """
        try:
            yield c['l']
            for c_2_data in c['c']:
                yield c_2_data

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_label_and_odds(self, c_2: list) -> Optional[tuple[str, float]]:
        """
        Extracts the label and odds from the C2 data.

        Args:
            c_2 (list): The C2 data.

        Returns:
            Optional[tuple[str, float]]: The label and odds if valid, otherwise None.
        """
        try:
            if len(c_2) > 3:
                if (label := c_2[1]) and (odds := c_2[2]):
                    return label.title(), float(odds)

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    async def _parse_betting_lines(self, resp: dict) -> None:
        """
        Parses betting lines from the response data.

        Args:
            resp (dict): The response data.
        """
        for section in self._get_sections(resp):
            if league := self._extract_league(section):
                for qg in self._get_qg_data(section):
                   q_data_iter = iter(self._get_q_data(qg, league))
                   if subject := next(q_data_iter, None):
                       if game := await self._get_game('PlayerProps', league, subject):
                           for q in q_data_iter:
                            c_data_iter = iter(self._get_c_data(qg, q, league))
                            if market := next(c_data_iter, None):
                                for c in c_data_iter:
                                    c_2_data_iter = iter(self._get_c_data_2(c))
                                    if line := next(c_2_data_iter, None):
                                        for c_2 in c_2_data_iter:
                                            label, odds = self._extract_label_and_odds(c_2)
                                            if label and odds:
                                                curr_datetime = datetime.now()
                                                betting_line_dict = {
                                                    'batch_timestamp': self.batch_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                                    'collection_timestamp': curr_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                                    'date': curr_datetime.strftime('%Y-%m-%d'),
                                                    'bookmaker': self.name,
                                                    'league': league,
                                                    'game': game,
                                                    'market_domain': "PlayerProps",
                                                    'market': market,
                                                    'subject': subject,
                                                    'label': label,
                                                    'line': line,
                                                    'odds': odds,
                                                }

                                                self._store_and_report(betting_line_dict)

    @collector_logger
    async def run_collector(self) -> None:
        """
        Runs the collector to gather betting lines data.
        """
        if await self._request_new_tokens():
            if contest_id := await self._request_contest_id():
                if betting_lines_resp := await self._request_betting_lines(contest_id):
                    await self._parse_betting_lines(betting_lines_resp)