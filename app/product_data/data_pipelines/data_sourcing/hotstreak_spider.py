import time
import uuid
import random
from datetime import datetime
from pymongo import MongoClient

import asyncio

from app.product_data.data_pipelines.utils import DataCleaner, DataNormalizer, RequestManager, Helper, get_db


class HotStreakSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='HotStreak')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []
        self.url = self.helper.get_url()
        self.headers = self.helper.get_headers()
        self.uniq_leagues = set()

    async def start(self):
        json_data = self.helper.get_json_data(name='leagues')
        await self.rm.post(self.url, self._parse_league_aliases, headers=self.headers, json=json_data)

    async def _parse_league_aliases(self, response):
        data = response.json().get('data')
        system = data.get('system')
        league_aliases = {
            league.get('name'): league.get('alias')
            for sport in system.get('sports', [])
            for league in sport.get('leagues', [])
            if league.get('name') and league.get('alias')
        }

        async def fetch_page(page):
            json_data = self.helper.get_json_data(var=page)
            page_response = await self.rm.post(self.url, self._parse_lines, league_aliases, headers=self.headers, json=json_data)
            if page_response and ((page_response.status_code == 500) or (page_response.status_code == 429)):
                raise Exception(f"[HotStreak]: Received status code 500 on page {i}. Breaking out of the loop.")

            return page_response

        tasks = []
        for i in range(1, random.randint(45, 50)):
            tasks.append(fetch_page(i))

        await asyncio.gather(*tasks)
        self.helper.store(self.prop_lines)
        print(self.uniq_leagues)

    async def _parse_lines(self, response, league_aliases):
        # get body content in json format
        data = response.json().get('data')
        search = data.get('search')
        # to get league ids
        leagues = dict()
        for league in search.get('leagueFilters', []):
            league_id, league_name = league.get('key'), league.get('name')
            if league_id and league_name:
                league_alias = league_aliases.get(league_name)
                if league_alias:
                    leagues[league_id] = league_alias
                else:
                    leagues[league_id] = league_name

        # to get opponent ids
        opponent_ids = dict()
        for game in search.get('games', []):
            league_id, game_time = game.get('leagueId'), game.get('scheduledAt')
            for opponent in game.get('opponents', []):
                opponent_id = opponent.get('id')
                if opponent_id:
                    opponent_ids[opponent_id] = {'league': leagues.get(league_id), 'game_time': game_time}

        # to get participant ids
        participants = dict()
        for player in search.get('participants', []):
            participant_id, opponent_id = player.get('id').split(':')[1], player.get('opponentId')
            player_data = player.get('player')
            if player_data:
                subject, position, number = player_data.get('displayName'), player_data.get('position'), player_data.get('number')
                participants[participant_id] = {'subject': subject, 'position': position, 'opponent_id': opponent_id, 'subject_number': number}

        # go through prop lines
        subject_ids = dict()

        for market in search.get('markets', []):
            market_id_components = market.get('id').split(':')[1:]
            more_components = ''.join(market_id_components).split(',')
            market_id, participant_id, the_market = None, more_components[0], more_components[1]

            # get player info and league
            subject_number, subject_id = None, None
            game_time, subject, position, league, participant = None, None, None, None, participants.get(participant_id)
            if participant:
                subject, position = participant.get('subject'), participant.get('position')
                opponent_id, subject_number = participant.get('opponent_id'), participant.get('subject_number')
                if opponent_id:
                    opponent = opponent_ids.get(opponent_id)
                    if opponent:
                        league, game_time = opponent.get('league'), opponent.get('game_time')
                        if league:
                            league = DataCleaner.clean_league(league)
                            self.uniq_leagues.add(league)

                if subject:
                    subject = subject.strip()
                    subject_id = subject_ids.get(f'{subject}{position}')
                    if not subject_id:
                        subject_id = self.dn.get_subject_id(subject, league, position=position, number=subject_number)
                        subject_ids[f'{subject}{position}'] = subject_id

            if the_market:
                if the_market == 'fantasy_points':
                    if league in {'NBA', 'WNBA'}:
                        the_market = 'basketball_fantasy_points'
                    elif league in {'NFL', 'NCAAF'}:
                        the_market = 'football_fantasy_points'

                market_id = self.dn.get_market_id(the_market)

            lines, probabilities = market.get('lines', []), market.get('probabilities', [])
            labels, n = ['Under', 'Over'], len(lines)
            if (lines and probabilities) and (n == len(probabilities)):
                for i in range(n):
                    line = lines[i]
                    for j in range(len(labels)):
                        probability = probabilities[i][j]
                        # convert probability to decimal odds
                        odds = round(1 / probability, 3)
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': the_market,
                            'game_time': game_time,
                            'position': position,
                            'subject_id': subject_id,
                            'subject': subject,
                            'jersey_number': subject_number,
                            'bookmaker': 'HotStreak',
                            'label': labels[j],
                            'line': line,
                            'odds': odds
                        })


async def main():
    db = get_db()
    spider = HotStreakSpider(uuid.uuid4(), RequestManager(), DataNormalizer('HotStreak', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[HotStreak]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
