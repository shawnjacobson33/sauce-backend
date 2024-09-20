import json
import time
import uuid
from datetime import datetime
from bs4 import BeautifulSoup
import asyncio

from app.product_data.data_pipelines.utils import RequestManager, DataCleaner, DataStandardizer, Helper, get_db, Subject


class DraftKingsPick6:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='DraftKingsPick6')
        self.rm = request_manager
        self.headers = self.helper.get_headers()
        self.ds = data_standardizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        await self.rm.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response):
        tasks, soup = [], BeautifulSoup(response.text, 'html.parser')
        for league in [sport_div.text for sport_div in soup.find_all('div', {'class': 'dkcss-7q5fzm'}) if
                       not sport_div.text.isnumeric()]:
            url = response.url + f"?sport={league}&_data=routes%2F_index"
            if league:
                league = DataCleaner.clean_league(league)
                if not Helper.is_league_good(league):
                    continue

            tasks.append(self.rm.get(url, self._parse_lines, league, headers=self.headers))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_lines(self, response, league):
        text = response.text

        # needs cleaning
        def clean_json():
            first_index = text.find('data:{')
            return text[:first_index]

        subject_ids = dict()
        data = json.loads(clean_json()).get('pickableIdToPickableMap')
        for pick in data.values():
            subject_id, line = None, None
            market_id, market, subject, subject_team, game_time, position = None, None, None, None, None, None
            pickable, active_market = pick.get('pickable'), pick.get('activeMarket')
            if pickable:
                market_category = pickable.get('marketCategory')
                if market_category:
                    market = market_category.get('marketName')
                    market_id = self.ds.get_market_id(market)
                for entity in pickable.get('pickableEntities', []):
                    subject, competitions = entity.get('displayName'), entity.get('pickableCompetitions')
                    if competitions:
                        first_competition = competitions[0]
                        position, team_data = first_competition.get('positionName'), first_competition.get('team')
                        # When secondary positions are also given
                        if position and '/' in position:
                            position = position.split('/')[0]

                        if team_data:
                            subject_team = team_data.get('abbreviation')

                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            cleaned_subj = DataCleaner.clean_subject(subject)
                            subject_obj = Subject(cleaned_subj, league, subject_team, position)
                            subject_id = self.ds.get_subject_id(subject_obj)
                            subject_ids[f'{subject}{subject_team}'] = subject_id

                        summary = first_competition.get('competitionSummary')
                        if summary:
                            game_time = summary.get('startTime')

            if active_market:
                line = active_market.get('targetValue')

            for label in ['Over', 'Under']:
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'game_time': game_time,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_team': subject_team,
                    'subject_id': subject_id,
                    'subject': subject,
                    'position': position,
                    'bookmaker': "DraftKingsPick6",
                    'label': label,
                    'line': line
                })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = DraftKingsPick6(batch_id, RequestManager(), DataStandardizer(batch_id, 'DraftKings Pick6', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[DraftKingsPick6]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
