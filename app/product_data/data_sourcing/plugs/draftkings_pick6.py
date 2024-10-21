from datetime import datetime
from bs4 import BeautifulSoup
import asyncio

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class DraftKingsPick6(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.headers = self.packager.get_headers()

    async def start(self):
        url = self.packager.get_url()
        await self.rm.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response):
        tasks, soup = [], BeautifulSoup(response.text, 'html.parser')
        for league in [sport_div.text for sport_div in soup.find_all('div', {'class': 'dkcss-7q5fzm'}) if
                       not sport_div.text.isnumeric()]:
            url = response.url + f"?sport={league}&_data=routes%2F_index"
            if league:
                league = clean_league(league)
                if not Packager.is_league_good(league):
                    continue

            tasks.append(self.rm.get(url, self._parse_lines, league, headers=self.headers))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league):
        subject_ids = dict()
        data = response.json().get('pickableIdToPickableMap')
        for pick in data.values():
            subject_id, line = None, None
            market_id, market, subject, subject_team, game_time, position = None, None, None, None, None, None
            pickable, active_market = pick.get('pickable'), pick.get('activeMarket')
            if pickable:
                market_category = pickable.get('marketCategory')
                if market_category:
                    market = market_category.get('marketName')
                    if market:
                        market = clean_market(market)
                        market_id = self.ds.get_market_id(Market(market, league))

                for entity in pickable.get('pickableEntities', []):
                    subject, competitions = entity.get('displayName'), entity.get('pickableCompetitions')
                    if competitions:
                        first_competition = competitions[0]
                        position, team_data = first_competition.get('positionName'), first_competition.get('team')
                        # When secondary positions are also given
                        if position and '/' in position:
                            position = clean_position(position.split('/')[0])

                        if team_data:
                            subject_team = team_data.get('abbreviation')

                        if subject:
                            subject = clean_subject(subject)
                            subject_id = subject_ids.get(f'{subject}{subject_team}')
                            if not subject_id:
                                subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                                subject_ids[f'{subject}{subject_team}'] = subject_id

            if active_market:
                line = active_market.get('targetValue')

            for label in ['Over', 'Under']:
                # update shared data
                PropLines.update(''.join(self.info.name.split()).lower(), {
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': self.info.name,
                    'label': label,
                    'line': line,
                    'odds': self.info.default_payout.odds
                })
                self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(DraftKingsPick6))
