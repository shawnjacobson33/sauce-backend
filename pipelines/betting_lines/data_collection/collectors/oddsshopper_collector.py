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

    def _get_payload_for_matchups(self) -> tuple[str, dict, dict]:
        """
        Gets the parameters for the matchups request.

        Returns:
            tuple[str, dict]: The url and headers for the matchups request.
        """
        try:
            url = self.payload['urls']['matchups']
            headers = self.payload['headers']
            cookies = self.payload['cookies']
            return url, headers, cookies

        except Exception as e:
            raise Exception(f'Failed to get payload for matchups: {e}')

    async def _request_matchups(self) -> dict | None:
        """
        Requests matchups data from the OddsShopper API.

        Returns:
            dict | None: The response data if successful, otherwise None.
        """
        try:
            url, headers, cookies = self._get_payload_for_matchups()
            if resp := await utils.requester.fetch(url, headers=headers, cookies=cookies):
                self.successful_requests += 1
                self.log_message(message='Successfully requested matchups', level='INFO')
                return resp

        except RequestingError as e:
            self.log_message(message=f'Failed to request matchups: {e}', level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(message=f'Failed to request matchups: {e}', level='EXCEPTION')

    def _get_offers(self, resp: dict) -> Iterable:
        """
        Extracts offers from the response data.

        Args:
            resp (dict): The response data.

        Yields:
            dict: The offer data.
        """
        try:
            for offer_category in resp['offerCategories']:
                if (category_name := offer_category['name']) in self.configs['valid_market_domains']:
                    for offer in offer_category['offers']:
                        offer['domain'] = category_name
                        yield offer

        except Exception as e:
            raise Exception(f'Failed to get offers: {e}')

    def _extract_league(self, offer: dict) -> str | None:
        """
        Extracts the league from the offer data.

        Args:
            offer (dict): The offer data.

        Returns:
            str | None: The cleaned league name if valid, otherwise None.
        """
        try:
            cleaned_league_name = offer['leagueCode'].strip().upper()
            return cleaned_league_name if cleaned_league_name in self.configs['valid_leagues'] else None

        except Exception as e:
            raise Exception(f'Failed to extract league: {e}')

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
            self.log_message(message=f'Failed to parse matchups: {e}', level='EXCEPTION')

    @staticmethod
    def _get_dates() -> tuple[str, str]:
        """
        Gets the current date and the date 8 days from now in the required format.

        Returns:
            tuple[str, str]: The current date and the date 8 days from now.
        """
        try:
            date_format = '%Y-%m-%dT%H:%M:%S.%f'
            return (datetime.now().strftime(date_format)[:-3] + 'Z',
                    (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z')

        except Exception as e:
            raise Exception(f'Failed to get dates: {e}')

    def _get_payload_for_betting_lines(self, offer: dict) -> tuple[str, dict, dict]:
        """
        Gets the parameters for the betting lines request.

        Returns:
            dict: The parameters for the betting lines request.
        """
        try:
            url = self.payload['urls']['betting_lines'].format(offer['id'])
            headers = self.payload['headers']
            params = self.payload['params']['betting_lines']
            start_date, end_date = self._get_dates()
            return url, headers, {**params, 'startDate': start_date, 'endDate': end_date}

        except Exception as e:
            raise Exception(f'Failed to get payload for betting lines: {e}')

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
                    self.log_message(
                        message=f'Successfully requested betting lines: {league} {offer['domain']}', level='INFO')
                    await self._parse_betting_lines(resp_json, league, offer['domain'])

        except RequestingError as e:
            self.log_message(message=f'Failed to request betting lines: {e}', level='ERROR')
            self.failed_requests += 1

        except Exception as e:
            self.log_message(message=f'Failed to request betting lines: {e}', level='EXCEPTION')

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
        try:
            return utils.get_sport(league) if market_domain == 'PlayerProps' else None

        except Exception as e:
            raise Exception(f'Failed to get sport: {market_domain} {e}')

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
            sport = self._get_sport(league, market_domain)
            std_market_name = self.standardizer.standardize_market_name(event['offerName'], market_domain, sport)
            return std_market_name

        except StandardizationError as e:
            self.log_message(message=f'Failed to standardize market: {e}', level='WARNING')
            self.failed_market_standardization += 1

        except Exception as e:
            self.log_message(message=f'Failed to extract market: {e}', level='EXCEPTION')

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
        try:
            for i, participant in enumerate(participants):
                if i != 1:
                    if ((market_domain == 'PlayerProps') and (i == 0)) or (market_domain == 'Gamelines'):
                        yield participant['name']

        except Exception as e:
            raise Exception(f'Failed to extract raw subject names: {e}')

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
            for raw_subject_name in self._extract_raw_subject_names(event['participants'], market_domain):
                cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                yield std_subject_name

                last_subject_name = std_subject_name

            while True:  # For player props with only one participant
                yield last_subject_name

        except StandardizationError as e:
            self.log_message(message=f'Failed to standardize subjects: {e}', level='WARNING')
            self.failed_subject_standardization += 1

        except Exception as e:
            self.log_message(message=f'Failed to extract subjects: {e}', level='EXCEPTION')

    def _extract_label(self, side: dict, market: str, market_domain: str) -> str | None:
        """
        Extracts the label from the side data.

        Args:
            side (dict): The side data.
            market (str): The market name.
            market_domain (str): The market domain.

        Returns:
            str | None: The label if valid, otherwise None.
        """
        try:
            if side['label'] != 'Draw':  # Todo: eases calculations for now
                return side['label'] if not ((market_domain == 'Gamelines') and ('Total' not in market)) else 'Over'

        except Exception as e:
            self.log_message(message=f'Failed to extract label: {e}', level='EXCEPTION')

    def _extract_line(self, outcome: dict, market: str) -> float | None:
        """
        Extracts the line from the outcome data.

        Args:
            outcome (dict): The outcome data.
            market (str): The market name.

        Returns:
            float | None: The line if valid, otherwise None.
        """
        try:
            return float(outcome['line']) if 'Moneyline' not in market else 0.5

        except Exception as e:
            self.log_message(message=f'Failed to extract line: {e}', level='EXCEPTION')

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
            assert isinstance(bookmaker, str)
            return bookmaker

        except Exception as e:
            self.log_message(message=f'Failed to extract bookmaker: {e}', level='EXCEPTION')

    def _extract_odds(self, outcome: dict) -> float | None:
        """
        Extracts the odds from the outcome data.

        Args:
            outcome (dict): The outcome data.

        Returns:
            float | None: The odds if valid, otherwise None.
        """
        try:
            return float(outcome['odds'])

        except Exception as e:
            self.log_message(message=f'Failed to extract odds: {e}', level='EXCEPTION')

    def _add_extra_source_stats(self, hold: float | None, outcome: dict, betting_line_dict: dict) -> None:
        """
        Adds extra source statistics to the betting line dictionary.

        Args:
            hold (float | None): The hold value.
            outcome (dict): The outcome data.
            betting_line_dict (dict): The betting line dictionary.
        """
        try:
            if url := outcome.get('deepLinkUrl'):
                betting_line_dict['url'] = url

            if hold:
                betting_line_dict.setdefault('extra_source_stats', {})['hold'] = round(hold, 3)

            if tw_prb := outcome.get('trueWinProbability'):
                betting_line_dict.setdefault('extra_source_stats', {})['tw_prb'] = round(tw_prb, 3)

            if ev := outcome.get('ev'):
                betting_line_dict.setdefault('extra_source_stats', {})['ev'] = round(ev, 3)

        except Exception as e:
            self.log_message(message=f'Failed to add extra source stats: {e}', level='EXCEPTION')

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
                                        if line := self._extract_line(outcome, market):
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
            self.log_message(message=f'Successfully parsed {len(self.items_container)} betting lines', level='INFO')
