import json
from datetime import datetime
import cloudscraper
import asyncio


class HotStreakSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.scraper = cloudscraper.create_scraper()
        self.headers = {
            'Host': 'api.hotstreak.gg',
            'accept': '*/*',
            'content-type': 'application/json',
            'authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJIb3RTdHJlYWsgKHByb2R1Y3Rpb24pIiwic3ViIjoiSHNmOjpVc2VyOnBydEpBNnciLCJleHAiOjE3MjcyMzQ4MjEsImlhdCI6MTcyNDU1NjQyMX0.ZCo5mvLX2DzDYQpBqXW2HG72kwl5EYC9VbXMKB6wRS8',
            'x-requested-with': 'ios',
            'user-agent': 'HotStreak/1717696638 CFNetwork/1496.0.7 Darwin/23.5.0',
            'accept-language': 'en-US,en;q=0.9',
        }

    async def start_requests(self):
        url = 'https://api.hotstreak.gg/graphql'
        json_data = {
            'query': 'query system { system {\n        __typename\ngeneratedAt\nmaximumDeposit\nmaximumInPlayWager\nmaximumPregameWager\nmaximumReferrals\nminimumDeposit\nminimumInPlayWager\nminimumPregameWager\nminimumWithdraw\npublicBroadcastChannel\npusherAppKey\npusherCluster\nreferrerBonus\nsports {\n\n__typename\nid\nactive\ncreatedAt\ngeneratedAt\ninPlay\nleagues {\n\n__typename\nid\nalias\ncreatedAt\ngeneratedAt\nname\novertimeClock\novertimePeriods\nregulationClock\nregulationPeriods\nsportId\nupdatedAt\n\n}\nname\nupdatedAt\n\n}\n\n      } }',
            'variables': {},
            'operationName': 'system',
        }

        response = await self.async_post(url, json=json_data)
        if response.status_code == 200:
            await self.parse_league_aliases(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def async_post(self, url, **kwargs):
        return await asyncio.to_thread(self.scraper.post, url, headers=self.headers, **kwargs)

    async def parse_league_aliases(self, response):
        data = response.json().get('data')

        system = data.get('system')
        league_aliases = {
            league.get('name'): league.get('alias')
            for sport in system.get('sports', [])
            for league in sport.get('leagues', [])
            if league.get('name') and league.get('alias')
        }

        tasks = []
        for i in range(1, 50):
            url = 'https://api.hotstreak.gg/graphql'
            json_data = {
                'query': 'query search($query: String, $page: Int, $gameFilter: [String!], $sportFilter: [String!], $teamFilter: [String!], $positionFilter: [String!], $categoryFilter: [String!], $promotedFilter: Boolean, $participantFilter: [String!], $leagueFilter: [String!]) { search(query: $query, page: $page, gameFilter: $gameFilter, sportFilter: $sportFilter, teamFilter: $teamFilter, positionFilter: $positionFilter, categoryFilter: $categoryFilter, promotedFilter: $promotedFilter, participantFilter: $participantFilter, leagueFilter: $leagueFilter) {\n        __typename\ngeneratedAt\ncategoryFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\ngameFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\ngames {\n\n__typename\nid\n... on EsportGame {\n\n__typename\nid\nminimumNumberOfGames\nvideogameTitle\n\n}\n... on GolfGame {\n\n__typename\nid\npairings {\n\n__typename\nid\nbackNine\ncreatedAt\ngameId\ngeneratedAt\nparticipantIds\nteeTime\nupdatedAt\n\n}\ntournament {\n\n__typename\nid\nname\n\n}\n\n}\nleagueId\nopponents {\n\n__typename\nid\ndesignation\ngameId\nteam {\n\n__typename\nid\ncreatedAt\ngeneratedAt\nlogoUrl\nmarket\nname\nshortName\nupdatedAt\n\n}\n\n}\nperiod\nreplay\nscheduledAt\nstatus\n\n}\nleagueFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nmarkets {\n\n__typename\nid\ngeneratedAt\nlines\noptions\nprobabilities\n\n}\nparticipants {\n\n__typename\nid\ncategories\nopponentId\nplayer {\n\n__typename\nid\ndisplayName\nexternalId\nfirstName\nheadshotUrl\ninjuries {\n\n__typename\nid\ncomment\ncreatedAt\ndescription\ngeneratedAt\nstatus\nstatusDate\nupdatedAt\n\n}\nlastName\nnickname\nnumber\nposition\nshortDisplayName\ntraits\n\n}\nposition\n\n}\npositionFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nsportFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nteamFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nstats\ntotalCount\n\n      } }',
                'variables': {
                    'query': '*',
                    'page': i,
                },
                'operationName': 'search',
            }

            tasks.append(self.fetch_and_parse_lines(url, json_data, league_aliases))

        await asyncio.gather(*tasks)

        with open('hotstreak_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))

    async def fetch_and_parse_lines(self, url, json_data, league_aliases):
        response = await self.async_post(url, json=json_data)
        if response.status_code == 200:
            await self.parse_lines(response, league_aliases)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def parse_lines(self, response, league_aliases):
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
                            'market': the_market,
                            'game_time': game_time,
                            'subject': subject,
                            'position': position,
                            'bookmaker': 'HotStreak',
                            'label': labels[j],
                            'line': line,
                            'odds': odds
                        })


async def main():
    spider = HotStreakSpider(batch_id='123')
    await spider.start_requests()

if __name__ == "__main__":
    asyncio.run(main())
