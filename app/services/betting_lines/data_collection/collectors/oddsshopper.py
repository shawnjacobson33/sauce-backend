import asyncio
import time
from datetime import datetime, timedelta
from typing import Optional, Iterable

from app.services.configs import load_configs
from app.services.utils import utilities as utils, Standardizer


class OddsShopperCollector:
    def __init__(self, collected_betting_lines: list[dict], standardizer: Standardizer):
        self.collected_betting_lines = collected_betting_lines
        self.standardizer = standardizer

        self.configs = load_configs('general')
        self.payload = utils.requester.get_payload(domain='betting_lines', source_name='OddsShopper')
        self.num_betting_lines_collected = 0

    async def _request_matchups(self) -> dict | None:
        url = self.payload['urls']['matchups']
        headers = self.payload['headers']
        cookies = self.payload['cookies']
        if resp_json := await utils.requester.fetch(url, headers=headers, cookies=cookies):
            return resp_json

    @staticmethod
    def _get_offers(resp: dict) -> Iterable:
        for offer_category in resp.get('offerCategories', []):
            if offer_category.get('name') == 'PlayerProps':
                for offer in offer_category.get('offers', []):
                    yield offer

    def _extract_league(self, offer: dict) -> str | None:
        if raw_league_name := offer.get('leagueCode'):
            cleaned_league_name = raw_league_name.strip().upper()
            if cleaned_league_name in self.configs['leagues_to_collect_from']:
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
        url = self.payload['urls']['betting_lines'].format(offer_id)
        headers = self.payload['headers']
        params = self._get_params()
        if resp_json := await utils.requester.fetch(url, headers=headers, params=params):
            self._parse_betting_lines(league, resp_json)


    def _extract_market(self, event: dict, league: str) -> str | None:
        try:
            if raw_market_name := event.get('offerName'):
                sport = utils.get_sport(league)
                std_market_name = self.standardizer.standardize_market_name(raw_market_name, sport)
                return std_market_name

        except Exception as e:
            print('[OddsShopper]: !! ERROR -', e, '!!')


    def _extract_subject(self, event: dict, league: str) -> str | None:
        try:
            if (participants := event.get('participants')) and (first_participants := participants[0]):
                if raw_subject_name := first_participants.get('name'):
                    cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                    subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                    std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                    return std_subject_name

                raise ValueError(f"No subject name found in event participants: '{event}'")

        except Exception as e:
            print('[OddsShopper]: !! ERROR -', e, '!!')

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


    def _parse_betting_lines(self, league: str, resp: dict) -> None:
        for event in resp:
            if market := self._extract_market(event, league):
                if subject := self._extract_subject(event, league):
                    for side in event.get('sides', []):
                        if label := side.get('label'):
                            for outcome in side.get('outcomes', []):
                                if bookmaker_name := self._extract_bookmaker(outcome):
                                    if odds := self._extract_odds(outcome):
                                        self.collected_betting_lines.append({
                                            'timestamp': datetime.now().isoformat(),
                                            'bookmaker': bookmaker_name,
                                            'league': league,
                                            'market': market,
                                            'subject': subject,
                                            'label': label,
                                            'line': float(outcome.get('line', 0.5)),
                                            'odds': odds,
                                        })
                                        self.num_betting_lines_collected += 1


    async def run_collector(self) -> None:
        try:
            start_time = time.time()
            print('[Oddsshopper]: Running collector...')
            print('[Oddsshopper]: Requesting matchups...')
            if matchups_resp := await self._request_matchups():
                print('[OddsShopper]: Matchups received...')
                betting_lines_tasks = []
                for event_data in self._parse_matchups(matchups_resp):
                    if event_data:
                        league, offer_id = event_data
                        betting_lines_tasks.append(self._request_betting_lines(league, offer_id))

                print('[Oddsshopper]: Requesting betting lines...')
                await asyncio.gather(*betting_lines_tasks)
                print('[Oddsshopper]: Betting lines received...')
                print(f'[Oddsshopper]: Collected {self.num_betting_lines_collected} betting lines...')
                self.num_betting_lines_collected = 0
                end_time = time.time()
                print(f'[Oddsshopper]: Time taken: {round(end_time - start_time, 2)} seconds')

        except Exception as e:
            print(e)


if __name__ == '__main__':
    collector = OddsShopperCollector([], Standardizer())  # Todo: how to get rosters dependency?
    asyncio.run(collector.run_collector())
