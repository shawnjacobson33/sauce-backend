import asyncio
from datetime import datetime, timedelta
from typing import Optional, Iterable

from urllib3.exceptions import ResponseError

from app.pipelines.utils import utilities as utils, Standardizer
from app.pipelines.betting_lines.data_collection.betting_lines_collector_base import BaseBettingLinesCollector


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
            if offer_category.get('name') in self.configs['valid_market_domains']:
                for offer in offer_category.get('offers', []):
                    yield offer

    def _extract_league(self, offer: dict) -> str | None:
        if raw_league_name := offer.get('leagueCode'):
            cleaned_league_name = raw_league_name.strip().upper()
            if cleaned_league_name in self.configs['valid_leagues']:
                return cleaned_league_name

    def _parse_matchups(self, resp: dict) -> tuple[str, str] | None:
        for offer in self._get_offers(resp):
            if (league := self._extract_league(offer)) and (offer_id := offer.get('id')):
                yield league, offer_id

    @staticmethod
    def _get_dates() -> tuple[str, str]:
        date_format = '%Y-%m-%dT%H:%M:%S.%f'
        return (datetime.now().strftime(date_format)[:-3] + 'Z',
                (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z')

    def _get_params(self):
        params = self.payload['params']['betting_lines']
        start_date, end_date = self._get_dates()
        return {**params, 'startDate': start_date, 'endDate': end_date}

    async def _request_betting_lines(self, league: str, offer_id: str) -> None:
        try:
            url = self.payload['urls']['betting_lines'].format(offer_id)
            headers = self.payload['headers']
            params = self._get_params()
            if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
                self.successful_requests += 1
                await self._parse_betting_lines(league, resp_json)

        except ResponseError as e:
            self.log_error(e)
            self.failed_requests += 1

    def _extract_market(self, event: dict, league: str) -> str | None:
        try:
            if raw_market_name := event.get('offerName'):
                if raw_market_name == 'Moneyline':
                    asd = 123
                sport = utils.get_sport(league)
                std_market_name = self.standardizer.standardize_market_name(raw_market_name, sport)
                return std_market_name
        
        except ValueError as e:  # Todo: implement custom error
            self.log_error(e)
            self.failed_market_standardization += 1

    def _extract_subject(self, event: dict, league: str) -> str | None:
        try:
            if (participants := event.get('participants')) and (first_participants := participants[0]):
                if raw_subject_name := first_participants.get('name'):
                    cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                    subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                    std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                    return std_subject_name
        
        except ValueError as e:  # Todo: implement custom error
            self.log_error(e)
            self.failed_subject_standardization += 1

    @staticmethod
    def _extract_bookmaker(outcome: dict) -> str | None:
        if bookmaker := outcome.get('sportsbookCode'):
            return bookmaker

    @staticmethod
    def _extract_odds(outcome: dict) -> Optional[float]:
        if odds := outcome.get('odds'):
            return odds

    @staticmethod
    def _extract_tw_prb(data: dict) -> Optional[float]:
        if tw_prob := data.get('trueWinProbability'):
            return tw_prob

    @staticmethod
    def extract_ev(data: dict) -> Optional[float]:
        if ev := data.get('ev'):
            return ev

    async def _parse_betting_lines(self, league: str, resp: dict) -> None:
        for event in resp:
            if market := self._extract_market(event, league):
                if subject := self._extract_subject(event, league):
                    if game := await self._get_game(league, subject):
                        for side in event.get('sides', []):
                            if label := side.get('label'):
                                for outcome in side.get('outcomes', []):
                                    if bookmaker_name := self._extract_bookmaker(outcome):
                                        if odds := self._extract_odds(outcome):
                                            curr_datetime = datetime.now()
                                            betting_line_dict = {  # Todo: better way to gradually build this dict?
                                                'batch_timestamp': self.batch_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                                'collection_timestamp': curr_datetime.strftime('%Y-%m-%d %H:%M:%S'),  # Todo: are you sure this is the format to use?
                                                'date': curr_datetime.strftime('%Y-%m-%d'),
                                                'bookmaker': bookmaker_name,
                                                'league': league,
                                                'game': game,
                                                'market': market,
                                                'subject': subject,
                                                'label': label,
                                                'line': float(outcome.get('line', 0.5)),
                                                'odds': odds,
                                            }
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

    @utils.logger.collector_logger(message='Running Collector')
    async def run_collector(self) -> None:
        if matchups_resp := await self._request_matchups():
            await self._gather_betting_lines_requests(matchups_resp)
