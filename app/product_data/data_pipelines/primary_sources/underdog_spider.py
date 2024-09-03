import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from pymongo import MongoClient
from app.product_data.data_pipelines.utils import DataCleaner as dc


from app.product_data.data_pipelines.utils.request_management import AsyncRequestManager
from pymongo.database import Database


class UnderdogSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']

    async def start(self):
        url = "https://api.underdogfantasy.com/beta/v5/over_under_lines"
        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9',
            'authorization': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjNnRTM4R1FUTW1lcVA5djFYVllEUCJ9.eyJ1ZF9zdWIiOiJhNWFlYzNhNy05YWZhLTQ5NjQtODhmMC0yMDg3YjZlMmI3MjAiLCJ1ZF9lbWFpbCI6ImphY29ic29uc2hhd24zM0BnbWFpbC5jb20iLCJ1ZF91c2VybmFtZSI6InNsaW1zaGFkeTMzMyIsImlzcyI6Imh0dHBzOi8vbG9naW4udW5kZXJkb2dzcG9ydHMuY29tLyIsInN1YiI6ImF1dGgwfGE1YWVjM2E3LTlhZmEtNDk2NC04OGYwLTIwODdiNmUyYjcyMCIsImF1ZCI6Imh0dHBzOi8vYXBpLnVuZGVyZG9nZmFudGFzeS5jb20iLCJpYXQiOjE3MjM1ODI1NjgsImV4cCI6MTcyMzU4NjE2OCwic2NvcGUiOiJvZmZsaW5lX2FjY2VzcyIsImd0eSI6WyJyZWZyZXNoX3Rva2VuIiwicGFzc3dvcmQiXSwiYXpwIjoiY1F2WXoxVDJCQUZiaXg0ZFlSMzdkeUQ5TzBUaGYxczYifQ.Bq5SXeHZ_tujrI-VQR3m7rTdyAquLwUYaxPoqZdo5BMRuHB4tQU4_i77A69PGRBaeiJ6n6qCRc_PZkRyl-atdcDzr5ewYsdwCzexNCNUKcOuSnLK1rF2ELz-sUkm8hWrCclUqSDWGnnl9ywDpbnVqMQqa3fQKZG9OOPObK77IFR0khoSK7MN2IrKbhi7JDnRvD8Sc7_iEfusjvpAc0K5lRVXIfWdXKeG0m7g6InKJQLR55vNUicSbsA40dQvdQyZuP_oRE5TcPe95z4ji9-7ut-Gl9_Er9oKs-2tIRyVCLTbjl5Ig_JIH6CszaUh887fRpfJSAdaO8eHfPsxxLyvHw',
            'client-device-id': '3ec5c852-be11-4848-a59c-a728187c7def',
            'client-request-id': '4264d803-5c8e-4d1b-90ce-794d695324fa',
            'client-type': 'web',
            'client-version': '20240813192008',
            'origin': 'https://underdogfantasy.com',
            'priority': 'u=1, i',
            'referer': 'https://underdogfantasy.com/',
            'referring-link': '',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'user-latitude': '20.6968263',
            'user-location-token': '',
            'user-longitude': '-156.441985',
        }

        await self.arm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()

        # first pass need to collect game/match info ids for each appearance
        game_ids, player_ids = dict(), dict()
        for appearance in data.get('appearances', []):
            appearance_id, appearance_match_id = appearance.get('id'), appearance.get('match_id')
            appearance_player_id = appearance.get('player_id')
            if appearance_id:
                if appearance_match_id:
                    game_ids[appearance_id] = appearance_match_id
                if appearance_player_id:
                    player_ids[appearance_id] = appearance_player_id

        # second pass to collect actual match data
        game_data = dict()
        for game in data.get('games', []):
            game_id = game.get('id')
            if game_id:
                game_data[game_id] = game

        # third pass to collect actual solo game data
        solo_game_data = dict()
        for game in data['solo_games']:
            game_id = game.get('id')
            if game_id:
                solo_game_data[game_id] = game

        # fourth pass to collect player data
        player_data = dict()
        for player in data.get('players', []):
            player_id = player.get('id')
            if player_id:
                player_data[player_id] = player

        for game in data.get('over_under_lines', []):
            line = game.get('stat_value')
            for option in game.get('options', []):
                # retrieve the match data corresponding to the appearance id of this line
                market_id, league, market, subject, game_time, over_under = '', '', '', '', '', game.get('over_under')
                if over_under:
                    appearance_stat = over_under.get('appearance_stat')
                    if appearance_stat:
                        appearance_id = appearance_stat.get('appearance_id')
                        market = appearance_stat.get('display_stat')

                        # get league and game time
                        if appearance_id:
                            game_id = game_ids.get(appearance_id)
                            if game_id:
                                match = game_data.get(game_id)
                                if not match:
                                    match = solo_game_data.get(game_id)

                                league, game_time = match.get('sport_id'), match.get('scheduled_at')
                                # don't want futures and don't want combos because they are niche and hard to normalize
                                if ('SZN' in league) or ('COMBOS' in league):
                                    continue

                        # get market and market id
                        if market:
                            market = market.strip()

                            # create more distinct markets
                            if market == 'Fantasy Points':
                                if league in {'NBA', 'WNBA'}:
                                    market = 'Basketball Fantasy Points'
                                elif league == 'MLB':
                                    market = 'Baseball Fantasy Points'
                                elif league in {'NFL', 'CFB'}:
                                    market = 'Football Fantasy Points'

                            market_id = self.msc.find_one({'Underdog Fantasy': market}, {'_id': 1})
                            if market_id:
                                market_id = market_id.get('_id')

                        # get subject
                        player_id = player_ids.get(appearance_id)
                        if player_id:
                            player = player_data.get(player_id)
                            # handle subject names
                            first_name, last_name = player.get('first_name'), player.get('last_name')
                            if not first_name:
                                subject = last_name
                            elif not last_name:
                                subject = first_name
                            else:
                                subject = ' '.join([first_name, last_name])

                            # fixes a formatting issues for ESPORTS subjects where Underdog formats like this:
                            # CS: [subject_name] and I only want the subject
                            if league == 'ESPORTS':
                                subject_components = subject.split(': ')
                                if subject_components and len(subject_components) == 2:
                                    # assign the league to the more specific game in the ESPORTS realm
                                    league, subject = subject_components[0], subject_components[1]

                        if league:
                            league = dc.clean_league(league)

                label = 'Over' if option.get('choice') == 'higher' else 'Under'
                multiplier = option.get('payout_multiplier')

                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'game_time': game_time,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market_name': market,
                    'subject': subject,
                    'bookmaker': 'Underdog Fantasy',
                    'label': label,
                    'line': line,
                    'multiplier': multiplier
                })

        relative_path = '../data_samples/underdog_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[Underdog Fantasy]: {len(self.prop_lines)} lines')


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = UnderdogSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Underdog Fantasy]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
