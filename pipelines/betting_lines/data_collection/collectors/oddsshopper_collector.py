import asyncio
from datetime import datetime, timedelta
from typing import Iterable

from pipelines.utils import utilities as utils
from pipelines.utils.standardization import Standardizer
from pipelines.utils.exceptions import StandardizationError, RequestingError
from pipelines.base.base_collector import collector_logger
from pipelines.betting_lines.data_collection.betting_lines_collector_base import BaseBettingLinesCollector


class OddsShopperCollector(BaseBettingLinesCollector):
    """
    A class to collect betting lines data from OddsShopper.

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
        Initializes the OddsShopperCollector with the given parameters.

        Args:
            batch_timestamp (datetime): The timestamp of the batch.
            betting_lines_container (list[dict]): The container to store betting lines.
            standardizer (Standardizer): The standardizer for data.
            configs (dict): The configuration settings.
        """
        super().__init__('OddsShopper', batch_timestamp, betting_lines_container, standardizer, configs)

    async def _request_matchups(self) -> dict | None:
        """
        Requests matchups data from the OddsShopper API.

        Returns:
            dict | None: The response data if successful, otherwise None.
        """
        try:
            url = self.payload['urls']['matchups']
            headers = self.payload['headers']
            cookies = self.payload['cookies']
            if resp := await utils.requester.fetch(url, headers=headers, cookies=cookies):
                self.successful_requests += 1
                return resp

        except RequestingError as e:
            self.log_message(e, level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _get_offers(self, resp: dict) -> Iterable:
        """
        Extracts offers from the response data.

        Args:
            resp (dict): The response data.

        Yields:
            dict: The offer data.
        """
        try:
            offer_categories = resp['offerCategories']
            for offer_category in offer_categories:
                if (category_name := offer_category['name']) in self.configs['valid_market_domains']:
                    for offer in offer_category['offers']:
                        offer['domain'] = category_name
                        yield offer

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_league(self, offer: dict) -> str | None:
        """
        Extracts the league from the offer data.

        Args:
            offer (dict): The offer data.

        Returns:
            str | None: The cleaned league name if valid, otherwise None.
        """
        raw_league_name = offer['leagueCode']
        cleaned_league_name = raw_league_name.strip().upper()
        return cleaned_league_name if cleaned_league_name in self.configs['valid_leagues'] else None

    def _parse_matchups(self, resp: dict) -> tuple[str, dict] | None:
        """
        Parses matchups from the response data.

        Args:
            resp (dict): The response data.

        Yields:
            tuple[str, dict]: The league and offer data.
        """
        try:
            for offer in self._get_offers(resp):
                if league := self._extract_league(offer):
                    yield league, offer

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    @staticmethod
    def _get_dates() -> tuple[str, str]:
        """
        Gets the current date and the date 8 days from now in the required format.

        Returns:
            tuple[str, str]: The current date and the date 8 days from now.
        """
        date_format = '%Y-%m-%dT%H:%M:%S.%f'
        return (datetime.now().strftime(date_format)[:-3] + 'Z',
                (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z')

    def _get_payload_for_betting_lines(self, offer: dict) -> tuple[str, dict, dict]:
        """
        Gets the parameters for the betting lines request.

        Returns:
            dict: The parameters for the betting lines request.
        """
        url = self.payload['urls']['betting_lines'].format(offer['id'])
        headers = self.payload['headers']
        params = self.payload['params']['betting_lines']
        start_date, end_date = self._get_dates()
        return url, headers, {**params, 'startDate': start_date, 'endDate': end_date}

    async def _request_betting_lines(self, league: str, offer: dict) -> None:
        """
        Requests betting lines data from the OddsShopper API.

        Args:
            league (str): The league name.
            offer (dict): The offer data.
        """
        try:
            url, headers, params = self._get_payload_for_betting_lines(offer)
            if url and headers and params:
                if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
                    self.successful_requests += 1
                    await self._parse_betting_lines(resp_json, league, offer['domain'])

        except RequestingError as e:
            self.log_message(e, level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    @staticmethod
    def _get_sport(league: str, market_domain: str) -> str | None:
        """
        Gets the sport based on the league and market domain.

        Args:
            league (str): The league name.
            market_domain (str): The market domain.

        Returns:
            str | None: The sport name if applicable, otherwise None.
        """
        return utils.get_sport(league) if market_domain == 'PlayerProps' else None

    def _extract_market(self, event: dict, league: str, market_domain: str) -> str | None:
        """
        Extracts the market from the event data.

        Args:
            event (dict): The event data.
            league (str): The league name.
            market_domain (str): The market domain.

        Returns:
            str | None: The standardized market name if valid, otherwise None.
        """
        try:
            raw_market_name = event['offerName']
            sport = self._get_sport(league, market_domain)
            std_market_name = self.standardizer.standardize_market_name(raw_market_name, market_domain, sport)
            return std_market_name

        except StandardizationError as e:
            self.log_message(e, level='WARNING')
            self.failed_market_standardization += 1

        except Exception as e:
            self.log_message(e, level='ERROR')

    @staticmethod
    def _extract_raw_subject_names(participants: list[dict], market_domain: str) -> Iterable:
        """
        Extracts raw subject names from the participants.

        Args:
            participants (list[dict]): The participants data.
            market_domain (str): The market domain.

        Yields:
            str: The raw subject name.
        """
        for i, participant in enumerate(participants):
            if i != 1:
                if ((market_domain == 'PlayerProps') and (i == 0)) or (market_domain == 'Gamelines'):
                    yield participant['name']

    def _extract_subjects(self, event: dict, league: str, market_domain: str) -> Iterable:
        """
        Extracts subjects from the event data.

        Args:
            event (dict): The event data.
            league (str): The league name.
            market_domain (str): The market domain.

        Yields:
            str: The standardized subject name.
        """
        try:
            last_subject_name = None
            participants = event.get('participants', [])
            for raw_subject_name in self._extract_raw_subject_names(participants, market_domain):
                cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                yield std_subject_name

                last_subject_name = std_subject_name

            while True:  # For player props with only one participant
                yield last_subject_name

        except StandardizationError as e:
            self.log_message(e, level='WARNING')
            self.failed_subject_standardization += 1

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    @staticmethod
    def _extract_label(side: dict, market: str, market_domain: str) -> str | None:
        """
        Extracts the label from the side data.

        Args:
            side (dict): The side data.
            market (str): The market name.
            market_domain (str): The market domain.

        Returns:
            str | None: The label if valid, otherwise None.
        """
        return side.get('label') if not ((market_domain == 'Gamelines') and ('Total' not in market)) else 'Over'

    def _extract_line(self, outcome: dict) -> float | None:
        """
        Extracts the line from the outcome data.

        Args:
            outcome (dict): The outcome data.

        Returns:
            float | None: The line if valid, otherwise None.
        """
        try:
            line = outcome['line']
            if isinstance(line, str):
                if line.isdecimal():
                    return float(line)

                return

            return line

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_bookmaker(self, outcome: dict) -> str | None:
        """
        Extracts the bookmaker from the outcome data.

        Args:
            outcome (dict): The outcome data.

        Returns:
            str | None: The bookmaker if valid, otherwise None.
        """
        try:
            bookmaker = outcome['sportsbookCode']
            if isinstance(bookmaker, str):
                return bookmaker

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    def _extract_odds(self, outcome: dict) -> float | None:
        """
        Extracts the odds from the outcome data.

        Args:
            outcome (dict): The outcome data.

        Returns:
            float | None: The odds if valid, otherwise None.
        """
        try:
            odds = outcome['odds']
            if isinstance(odds, (int, float)):
                return odds

            elif isinstance(odds, str):
                if odds.isdecimal():
                    return float(odds)

        except Exception as e:
            self.log_message(e, level='EXCEPTION')

    @staticmethod
    def _add_extra_source_stats(hold: float | None, outcome: dict, betting_line_dict: dict) -> None:
        """
        Adds extra source statistics to the betting line dictionary.

        Args:
            hold (float | None): The hold value.
            outcome (dict): The outcome data.
            betting_line_dict (dict): The betting line dictionary.
        """
        if url := outcome.get('deepLinkUrl'):
            betting_line_dict['url'] = url

        if hold:
            betting_line_dict.setdefault('extra_source_stats', {})['hold'] = round(hold, 3)

        if tw_prb := outcome.get('trueWinProbability'):
            betting_line_dict.setdefault('extra_source_stats', {})['tw_prb'] = round(tw_prb, 3)

        if ev := outcome.get('ev'):
            betting_line_dict.setdefault('extra_source_stats', {})['ev'] = round(ev, 3)

    async def _parse_betting_lines(self, resp: dict, league: str, market_domain: str) -> None:
        """
        Parses betting lines from the response data.

        Args:
            resp (dict): The response data.
            league (str): The league name.
            market_domain (str): The market domain.
        """
        for event in resp:
            hold = event.get('hold')
            if market := self._extract_market(event, league, market_domain):
                if subjects := self._extract_subjects(event, league, market_domain):
                    subjects_iter = iter(subjects)
                    for side in event.get('sides', []):
                        if subject := next(subjects_iter, None):
                            if game := await self._get_game(market_domain, league, subject):
                                if label := self._extract_label(side, market, market_domain):
                                    for outcome in side.get('outcomes', []):
                                        if line := self._extract_line(outcome):
                                            if bookmaker := self._extract_bookmaker(outcome):
                                                if odds := self._extract_odds(outcome):
                                                    collection_datetime = datetime.now()
                                                    betting_line_dict = {
                                                        'batch_timestamp': self.batch_timestamp.strftime(
                                                            '%Y-%m-%d %H:%M:%S'),
                                                        'collection_timestamp': collection_datetime.strftime(
                                                            '%Y-%m-%d %H:%M:%S'),
                                                        # Todo: are you sure this is the format to use?
                                                        'date': collection_datetime.strftime('%Y-%m-%d'),
                                                        'bookmaker': bookmaker,
                                                        'league': league,
                                                        'game': game,
                                                        'market_domain': market_domain,
                                                        'market': market,
                                                        'subject': subject,
                                                        'label': label,
                                                        'line': line,
                                                        'odds': odds,
                                                    }

                                                    self._add_extra_source_stats(hold, outcome, betting_line_dict)
                                                    self._store_and_report(betting_line_dict)

    async def _gather_betting_lines_requests(self, matchups_resp: dict):
        """
        Gathers betting lines requests based on the matchups response.

        Args:
            matchups_resp (dict): The matchups response data.
        """
        betting_lines_tasks = []
        for event_data in self._parse_matchups(matchups_resp):
            league, offer_id = event_data
            betting_lines_tasks.append(self._request_betting_lines(league, offer_id))

        await asyncio.gather(*betting_lines_tasks)

    @collector_logger
    async def run_collector(self) -> None:
        """
        Runs the collector to gather betting lines data.
        """
        if matchups_resp := await self._request_matchups():
            await self._gather_betting_lines_requests(matchups_resp)
