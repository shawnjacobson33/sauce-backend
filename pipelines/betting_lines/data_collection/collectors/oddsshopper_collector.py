import asyncio
from datetime import datetime, timedelta
from typing import Iterable

from urllib3.exceptions import ResponseError

from pipelines.utils import Standardizer
from pipelines.utils import utilities as utils

from pipelines.base.base_collector import collector_logger
from pipelines.betting_lines.data_collection.betting_lines_collector_base import BaseBettingLinesCollector


class OddsShopperCollector(BaseBettingLinesCollector):

    def __init__(self,
                 batch_timestamp: datetime,
                 betting_lines_container: list[dict],
                 standardizer: Standardizer,
                 configs: dict):

        super().__init__('OddsShopper', batch_timestamp, betting_lines_container, standardizer, configs)

    async def _request_matchups(self) -> dict | None:
        try:
            url = self.payload['urls']['matchups']
            headers = self.payload['headers']
            cookies = self.payload['cookies']
            if resp := await utils.requester.fetch(url, headers=headers, cookies=cookies):
                self.successful_requests += 1
                return resp

        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1

    def _get_offers(self, resp: dict) -> Iterable:
        offer_categories = resp.get('offerCategories', [])
        for offer_category in offer_categories:
            if (category_name := offer_category['name']) in self.configs['valid_market_domains']:
                for offer in offer_category.get('offers', []):
                    offer['domain'] = category_name
                    yield offer

    def _extract_league(self, offer: dict) -> str | None:
        if raw_league_name := offer.get('leagueCode'):
            cleaned_league_name = raw_league_name.strip().upper()
            if cleaned_league_name in self.configs['valid_leagues']:
                return cleaned_league_name

    def _parse_matchups(self, resp: dict) -> tuple[str, dict] | None:
        for offer in self._get_offers(resp):
            if league := self._extract_league(offer):
                yield league, offer

    @staticmethod
    def _get_dates() -> tuple[str, str]:
        date_format = '%Y-%m-%dT%H:%M:%S.%f'
        return (datetime.now().strftime(date_format)[:-3] + 'Z',
                (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z')

    def _get_params(self):
        params = self.payload['params']['betting_lines']
        start_date, end_date = self._get_dates()
        return {**params, 'startDate': start_date, 'endDate': end_date}

    async def _request_betting_lines(self, league: str, offer: dict) -> None:
        try:
            url = self.payload['urls']['betting_lines'].format(offer['id'])
            headers = self.payload['headers']
            params = self._get_params()
            if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
                self.successful_requests += 1
                await self._parse_betting_lines(resp_json, league, offer['domain'])

        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1

        # except AttributeError as e:
        #     self.log_error(e)

    def _extract_market(self, event: dict, league: str, market_domain: str) -> str | None:
        try:
            raw_market_name = event['offerName']
            sport = utils.get_sport(league) if market_domain == 'PlayerProps' else None
            std_market_name = self.standardizer.standardize_market_name(raw_market_name, market_domain, sport)
            return std_market_name
        
        except ValueError as e:  # Todo: implement custom error
            self.log_error(e)
            self.failed_market_standardization += 1

        # except AttributeError as e:
        #     self.log_error(e)

    @staticmethod
    def _extract_raw_subject_names(participants: list[dict], market_domain: str) -> Iterable:
        for i, participant in enumerate(participants):
            if i != 1:
                if ((market_domain == 'PlayerProps') and (i == 0)) or (market_domain == 'Gamelines'):
                    yield participant['name']

    def _extract_subjects(self, event: dict, league: str, market_domain: str) -> Iterable:
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
        
        except ValueError as e:  # Todo: implement custom error
            self.log_error(e)
            self.failed_subject_standardization += 1

    @staticmethod
    def _extract_label(side: dict, market: str, market_domain: str) -> str | None:
        return side.get('label') if not ((market_domain == 'Gamelines') and ('Total' not in market)) else 'Over'

    @staticmethod
    def _add_extra_source_stats(hold: float | None, outcome: dict, betting_line_dict: dict):
        if hold:
            betting_line_dict.setdefault('extra_source_stats', {})['hold'] = round(hold, 3)

        if tw_prb := outcome.get('trueWinProbability'):
            betting_line_dict.setdefault('extra_source_stats', {})['tw_prb'] = round(tw_prb, 3)

        if ev := outcome.get('ev'):
            betting_line_dict.setdefault('extra_source_stats', {})['ev'] = round(ev, 3)

    async def _parse_betting_lines(self, resp: dict, league: str, market_domain: str) -> None:
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
                                        if bookmaker_name := outcome.get('sportsbookCode'):
                                            if odds := outcome.get('odds'):
                                                curr_datetime = datetime.now()
                                                betting_line_dict = {  # Todo: better way to gradually build this dict?
                                                    'batch_timestamp': self.batch_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                                    'collection_timestamp': curr_datetime.strftime('%Y-%m-%d %H:%M:%S'),  # Todo: are you sure this is the format to use?
                                                    'date': curr_datetime.strftime('%Y-%m-%d'),
                                                    'bookmaker': bookmaker_name,
                                                    'league': league,
                                                    'game': game,
                                                    'market_domain': market_domain,
                                                    'market': market,
                                                    'subject': subject,
                                                    'label': label,
                                                    'line': float(outcome.get('line', 0.5)),
                                                    'odds': odds,
                                                }
                                                if url := outcome.get('deepLinkUrl'):
                                                    betting_line_dict['url'] = url

                                                self._add_extra_source_stats(hold, outcome, betting_line_dict)
                                                betting_line_key = utils.storer.get_betting_line_key(betting_line_dict)
                                                betting_line_dict['_id'] = betting_line_key
                                                self.items_container.append(betting_line_dict)
                                                self.num_collected += 1

    async def _gather_betting_lines_requests(self, matchups_resp: dict):
        betting_lines_tasks = []
        for event_data in self._parse_matchups(matchups_resp):
            if event_data:
                league, offer_id = event_data
                betting_lines_tasks.append(self._request_betting_lines(league, offer_id))

        await asyncio.gather(*betting_lines_tasks)

    @collector_logger
    async def run_collector(self) -> None:
        if matchups_resp := await self._request_matchups():
            await self._gather_betting_lines_requests(matchups_resp)
