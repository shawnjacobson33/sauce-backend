import main
import random
from datetime import datetime
import asyncio

from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_manipulation import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class HotStreak(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []
        self.url = self.packager.get_url()
        self.headers = self.packager.get_headers()

    async def start(self):
        json_data = self.packager.get_json_data(name='leagues')
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
            json_data = self.packager.get_json_data(var=page)
            page_response = await self.rm.post(self.url, self._parse_lines, league_aliases, headers=self.headers, json=json_data)
            if page_response and ((page_response.status_code == 500) or (page_response.status_code == 429)):
                raise Exception(f"[HotStreak]: Received status code 500 on page {i}. Breaking out of the loop.")

            return page_response

        tasks = []
        for i in range(1, random.randint(45, 50)):
            tasks.append(fetch_page(i))

        await asyncio.gather(*tasks)
        self.packager.store(self.prop_lines)

    async def _parse_lines(self, response, league_aliases):
        # get body content in json format
        data = response.json().get('data')
        if data:
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
                jersey_number, subject_id = None, None
                game_time, subject, position, league, participant = None, None, None, None, participants.get(participant_id)
                if participant:
                    subject, position = participant.get('subject'), participant.get('position')
                    if position:
                        position = clean_position(position)

                    opponent_id, jersey_number = participant.get('opponent_id'), participant.get('subject_number')
                    jersey_number = str(jersey_number) if jersey_number is not None else jersey_number
                    if opponent_id:
                        opponent = opponent_ids.get(opponent_id)
                        if opponent:
                            league = opponent.get('league')
                            if league:
                                league = clean_league(league)
                                if not Packager.is_league_good(league):
                                    continue

                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{position}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, league, position=position, jersey_number=jersey_number))
                            subject_ids[f'{subject}{position}'] = subject_id

                if the_market:
                    if the_market == 'fantasy_points':
                        if league in {'NBA', 'WNBA'}:
                            the_market = 'Basketball Fantasy Points'
                        elif league in {'NFL', 'NCAAF'}:
                            the_market = 'Football Fantasy Points'

                    the_market = clean_market(the_market)
                    market_id = self.ds.get_market_id(Market(the_market, league))

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
                                'subject_id': subject_id,
                                'subject': subject,
                                'bookmaker': self.info.name,
                                'label': labels[j],
                                'line': line,
                                'odds': odds
                            })


if __name__ == "__main__":
    asyncio.run(main.run(HotStreak))
