import asyncio
import json
import os
import time
import uuid
import re
from datetime import datetime

import pandas as pd

from app.product_data.data_pipelines.request_management import AsyncRequestManager


class SmartBettorSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm

    async def start(self):
        # going to get the odds for all in-season leagues
        url = "https://smartbettor.ai/api/get_pos_ev_data_react"
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Referer': 'https://smartbettor.ai/positive-ev',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        }

        await self.arm.get(url, self._parse_odds, headers=headers)

    async def _parse_odds(self, response):
        data = response.json()

        bookmaker_keys = {
            "betonlineag": "BetOnline", "betmgm": "BetMGM", "betrivers": "BetRivers", "betus": "BetUS",
            "bovada": "Bovada",
            "draftkings": "DraftKings", "fanduel": "FanDuel", "lowvig": "LowVig", "mybookieag": "MyBookie",
            "pointsbetus": "PointsBetUS", "superbook": "SuperBook", "twinspires": "TwinSpires", "unibet_us": "UnibetUS",
            "williamhill_us": "WilliamHillUS", "wynnbet": "WynnBET", "betparx": "betPARX", "espnbet": "ESPNBet",
            "fliff": "Fliff", "sisportsbook": "SISportsbook", "tipico_us": "Tipico", "windcreek": "WindCreek",
            "betvictor": "BetVictor", "betway": "Betway", "boylesports": "BoyleSports", "casumo": "Casumo",
            "coral": "Coral",
            "grosvenor": "Grosvenor", "leovegas": "LeoVegas", "livescorebet": "LiveScore Bet", "matchbook": "Matchbook",
            "mrgreen": "Mr Green", "paddypower": "Paddy Power", "skybet": "Sky Bet", "unibet_uk": "UnibetUK",
            "virginbet": "Virgin Bet", "williamhill": "William Hill", "onexbet": "1XBET", "sport888": "888 Sport",
            "betclic": "BetClic", "betsson": "Betsson", "coolbet": "Coolbet", "everygame": "Everygame",
            "livescorebet_eu": "LiveScoreEU Bet", "marathonbet": "Marathonbet", "nordicbet": "Nordicbet",
            "pinnacle": "Pinnacle", "suprabets": "Suprabets", "unibet_eu": "UnibetEU", "betr_au": "BetrAU",
            "bluebet": "BlueBet", "ladbrokes_au": "LadbrokesAU", "neds": "Neds", "playup": "PlayUp",
            "pointsbetau": "PointsBetAU",
            "sportsbet": "Sportsbet", "tab": "TAB", "unibet": "Unibet", "ballybet": "BallyBet", "gtbets": "GTbets",
            "hardrockbet": "HardRock", "betanysports": "BetAnySports"
        }
        for event in data:
            last_updated, league = event.get('time_difference_formatted'), event.get('sport_league_display')
            game_time, market = event.get('game_date'), event.get('market_display').strip()
            bet_type, home_team, away_team = event.get('bet_type'), event.get('home_team'), event.get('away_team')

            for subject_key in ['wager_display', 'wager_display_other']:
                subject = event.get(subject_key)

                if subject_key == 'wager_display_other':
                    line = event.get('outcome_point_other')
                    if not line:
                        line = '0.5'
                else:
                    line = event.get('outcome_point')
                    if not line:
                        line = '0.5'

                for bookmaker_key, bookmaker_name in bookmaker_keys.items():
                    if (bookmaker_key not in event) or (f"{bookmaker_key}_other" not in event):
                        continue

                    # get market
                    if 'Moneylne' in market:
                        market = 'Moneyline'

                    # get market category
                    market_category = 'game_lines'
                    if ('Spread' not in market) and ('Total' not in market) and ('Moneyline' not in market):
                        market_category = 'player_props'

                    # weird edge case for soccer betting where the wager display only includes the subject
                    if market != 'Draw No Bet':
                        subject_components = subject.split()
                        if subject_components:
                            if (len(subject_components) > 2) and ((subject_components[-2] == 'Over') or (
                                    subject_components[-2] == 'Under')):
                                # get rid of extra parts
                                subject = ' '.join(subject_components[:-2])
                            # Game Totals Market
                            elif (subject_components[0] == 'Over') or (subject_components[0] == 'Under'):
                                subject = f"{home_team} vs. {away_team}"
                            # Spread Market
                            elif ("+" in subject) or ("-" in subject):
                                subject = ' '.join(subject_components[:-1])

                    # get odds, label and line
                    if subject_key == 'wager_display_other':
                        odds, label = event.get(f'{bookmaker_key}_other'), event.get('outcome_name_other')
                    else:
                        odds, label = event.get(bookmaker_key), event.get('outcome_name')

                    if (not odds) or (odds == 0.0):
                        continue

                    # label would be empty otherwise
                    if bet_type == 'moneyline':
                        label = 'Moneyline'
                    # Label would otherwise be -3.5
                    elif label not in {'Over', 'Under'}:
                        label = 'Spread'
                        # Keep with spread syntax
                        if (bool(re.match(r'^-?\d+(\.\d+)?$', line))) and (float(line) > 0):
                            line = f'+{line}'

                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'last_updated': last_updated,
                        'league': league,
                        'game_time': game_time,
                        'market_category': market_category,
                        'market': market,
                        'subject': subject,
                        'bookmaker': bookmaker_name,
                        'label': label,
                        'line': line,
                        'odds': odds
                    })

        # self.count_lines_per_bookmaker()
        relative_path = 'data_samples/smartbettor_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[SmartBettor]: {len(self.prop_lines)} lines')

    def count_lines_per_bookmaker(self):
        df = pd.DataFrame(self.prop_lines)

        print(df.groupby('bookmaker').size().reset_index())


async def main():
    spider = SmartBettorSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager())
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[SmartBettor]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
