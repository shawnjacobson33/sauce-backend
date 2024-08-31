import json
import os
import time
import uuid
import random
from datetime import datetime
from pymongo import MongoClient

import asyncio

from app.product_data.data_pipelines.request_management import AsyncRequestManager
from pymongo.collection import Collection


class HotStreakSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, msc: Collection):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm
        self.msc = msc
        self.headers = {
            'Host': 'api.hotstreak.gg',
            'accept': '*/*',
            'content-type': 'application/json',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJIb3RTdHJlYWsgKHByb2R1Y3Rpb24pIiwic3ViIjoiSHNmOjpVc2VyOnBydEpBNnciLCJleHAiOjE3MjcyMzQ4MjEsImlhdCI6MTcyNDU1NjQyMX0.ZCo5mvLX2DzDYQpBqXW2HG72kwl5EYC9VbXMKB6wRS8',
            'x-requested-with': 'ios',
            'user-agent': 'HotStreak/1717696638 CFNetwork/1496.0.7 Darwin/23.5.0',
            'accept-language': 'en-US,en;q=0.9',
        }

    async def start(self):
        url = 'https://api.hotstreak.gg/graphql'
        json_data = {
            'query': 'query system { system {\n        __typename\ngeneratedAt\nmaximumDeposit\nmaximumInPlayWager\nmaximumPregameWager\nmaximumReferrals\nminimumDeposit\nminimumInPlayWager\nminimumPregameWager\nminimumWithdraw\npublicBroadcastChannel\npusherAppKey\npusherCluster\nreferrerBonus\nsports {\n\n__typename\nid\nactive\ncreatedAt\ngeneratedAt\ninPlay\nleagues {\n\n__typename\nid\nalias\ncreatedAt\ngeneratedAt\nname\novertimeClock\novertimePeriods\nregulationClock\nregulationPeriods\nsportId\nupdatedAt\n\n}\nname\nupdatedAt\n\n}\n\n      } }',
            'variables': {},
            'operationName': 'system',
        }

        await self.arm.post(url, self._parse_league_aliases, headers=self.headers, json=json_data)

    async def _parse_league_aliases(self, response):
        data = response.json().get('data')

        system = data.get('system')
        league_aliases = {
            league.get('name'): league.get('alias')
            for sport in system.get('sports', [])
            for league in sport.get('leagues', [])
            if league.get('name') and league.get('alias')
        }

        url = 'https://api.hotstreak.gg/graphql'

        async def fetch_page(page):
            json_data = {
                'query': 'query search($query: String, $page: Int, $gameFilter: [String!], $sportFilter: [String!], $teamFilter: [String!], $positionFilter: [String!], $categoryFilter: [String!], $promotedFilter: Boolean, $participantFilter: [String!], $leagueFilter: [String!]) { search(query: $query, page: $page, gameFilter: $gameFilter, sportFilter: $sportFilter, teamFilter: $teamFilter, positionFilter: $positionFilter, categoryFilter: $categoryFilter, promotedFilter: $promotedFilter, participantFilter: $participantFilter, leagueFilter: $leagueFilter) {\n        __typename\ngeneratedAt\ncategoryFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\ngameFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\ngames {\n\n__typename\nid\n... on EsportGame {\n\n__typename\nid\nminimumNumberOfGames\nvideogameTitle\n\n}\n... on GolfGame {\n\n__typename\nid\npairings {\n\n__typename\nid\nbackNine\ncreatedAt\ngameId\ngeneratedAt\nparticipantIds\nteeTime\nupdatedAt\n\n}\ntournament {\n\n__typename\nid\nname\n\n}\n\n}\nleagueId\nopponents {\n\n__typename\nid\ndesignation\ngameId\nteam {\n\n__typename\nid\ncreatedAt\ngeneratedAt\nlogoUrl\nmarket\nname\nshortName\nupdatedAt\n\n}\n\n}\nperiod\nreplay\nscheduledAt\nstatus\n\n}\nleagueFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nmarkets {\n\n__typename\nid\ngeneratedAt\nlines\noptions\nprobabilities\n\n}\nparticipants {\n\n__typename\nid\ncategories\nopponentId\nplayer {\n\n__typename\nid\ndisplayName\nexternalId\nfirstName\nheadshotUrl\ninjuries {\n\n__typename\nid\ncomment\ncreatedAt\ndescription\ngeneratedAt\nstatus\nstatusDate\nupdatedAt\n\n}\nlastName\nnickname\nnumber\nposition\nshortDisplayName\ntraits\n\n}\nposition\n\n}\npositionFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nsportFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nteamFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nstats\ntotalCount\n\n      } }',
                'variables': {
                    'query': '*',
                    'page': page,
                },
                'operationName': 'search',
            }

            page_response = await self.arm.post(url, self._parse_lines, league_aliases, headers=self.headers, json=json_data)
            if page_response and ((page_response.status_code == 500) or (page_response.status_code == 429)):
                raise Exception(f"[HotStreak]: Received status code 500 on page {i}. Breaking out of the loop.")

            return page_response

        tasks = []
        for i in range(1, random.randint(45, 55)):
            tasks.append(fetch_page(i))

        await asyncio.gather(*tasks)

        relative_path = '../data_samples/hotstreak_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[HotStreak]: {len(self.prop_lines)} lines')

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
                subject, position = player_data.get('displayName'), player_data.get('position')
                participants[participant_id] = {'subject': subject, 'position': position, 'opponent_id': opponent_id}

        # go through prop lines
        for market in search.get('markets', []):
            market_id_components = market.get('id').split(':')[1:]
            more_components = ''.join(market_id_components).split(',')
            participant_id, the_market = more_components[0], more_components[1]
            market_id = self.msc.find_one({'HotStreak': the_market}, {'_id': 1})
            if market_id:
                market_id = market_id.get('_id')

            # get player info and league
            game_time, subject, position, league, participant = '', '', '', '', participants.get(participant_id)
            if participant:
                subject, position = participant.get('subject'), participant.get('position')
                opponent_id = participant.get('opponent_id')
                if opponent_id:
                    opponent = opponent_ids.get(opponent_id)
                    if opponent:
                        league, game_time = opponent.get('league'), opponent.get('game_time')

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
                            'market_name': the_market,
                            'game_time': game_time,
                            'subject': subject,
                            'position': position,
                            'bookmaker': 'HotStreak',
                            'label': labels[j],
                            'line': line,
                            'odds': odds
                        })


async def main():
    client = MongoClient('mongodb://localhost:27017/')

    db = client['sauce']

    spider = HotStreakSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), msc=db['markets'])
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[HotStreak]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
