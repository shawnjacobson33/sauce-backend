import time
import uuid
from datetime import datetime
import asyncio

from app.product_data.data_pipelines.utils import clean_subject, clean_league, DataStandardizer, RequestManager, \
    Helper, get_db, Subject, Market


class PaydaySpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='Payday')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []
        self.headers = self.helper.get_headers()

    async def start(self):
        url = self.helper.get_url(name='leagues')
        params = self.helper.get_params(name='leagues')
        await self.rm.get(url, self._parse_leagues, headers=self.headers, params=params)

    async def _parse_leagues(self, response):
        data = response.json()
        url = self.helper.get_url(name='contests')
        tasks = []
        for league_data in data.get('data', []):
            league = league_data.get('slug')
            if league:
                if not Helper.is_league_good(clean_league(league)):
                    continue

                params = self.helper.get_params(name='contests', var_1=league)
                league = clean_league(league)
                tasks.append(self.rm.get(url, self._parse_contests, league, headers=self.headers, params=params))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)

    async def _parse_contests(self, response, league):
        data = response.json()
        contests = data.get('data', {}).get('contests')
        if contests:
            parlay_contest = contests[0]
            if 'Parlay Contest' in parlay_contest.get('name'):
                parlay_contest_id = parlay_contest.get('id')
                if parlay_contest_id:
                    url = self.helper.get_url().format(parlay_contest_id)
                    await self.rm.get(url, self._parse_lines, league, headers=self.headers)

    async def _parse_lines(self, response, league):
        data = response.json()
        subject_ids = dict()
        for game in data.get('data', {}).get('games', []):
            game_info = game.get('title')
            # Need team ids and names for player info
            teams_dict, home_team, away_team = dict(), game.get('home_team'), game.get('away_team')
            if home_team and away_team:
                for team in [home_team, away_team]:
                    team_id, team_name = team.get('id'), team.get('code')
                    if team_id and team_name:
                        teams_dict[team_id] = team_name.upper()

            for player_prop in game.get('player_props', []):
                market_id = None
                market, line, player = player_prop.get('name'), player_prop.get('value'), player_prop.get('player')
                if market:
                    market_id = self.ds.get_market_id(Market(market, league))

                if player:
                    jersey_number = player.get('number')
                    # for tennis players that don't actually have jersey numbers
                    jersey_number = None if jersey_number == 'N/A' else jersey_number
                    subject, position = player.get('name'), player.get('position')
                    subject_id, subject_team, team_id = None, None, player.get('team_id')
                    if team_id:
                        subject_team = teams_dict.get(team_id)

                    if subject:
                        # For tennis players when they don't actually have a team
                        if subject_team == subject.upper():
                            subject_team = None

                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            cleaned_subject = clean_subject(subject)
                            subject_id = self.ds.get_subject_id(Subject(cleaned_subject, league, subject_team, position, jersey_number))
                            subject_ids[f'{subject}{subject_team}'] = subject_id

                    for label in ['Over', 'Under']:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': market,
                            'subject_team': subject_team,
                            'position': position,
                            'subject_id': subject_id,
                            'subject': subject,
                            'jersey_number': jersey_number,
                            'bookmaker': 'Payday',
                            'label': label,
                            'line': line
                        })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = PaydaySpider(batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Payday]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
