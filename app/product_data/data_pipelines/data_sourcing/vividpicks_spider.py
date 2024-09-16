import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import DataCleaner, RequestManager, DataNormalizer, Helper, get_db


class VividPicksSpider:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_normalizer: DataNormalizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='VividPicks')
        self.rm = request_manager
        self.dn = data_normalizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        headers = self.helper.get_headers()
        json_data = self.helper.get_json_data()
        await self.rm.post(url, self._parse_lines, headers=headers, json=json_data)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()
        subject_ids = dict()
        for event in data.get('gret', []):
            league, game_info, game_time = event.get('league'), event.get('gameInfo'), event.get('gameTime')
            if 'Futures' in game_info:
                continue

            if league:
                league = DataCleaner.clean_league(league)

            for player in event.get('activePlayers', []):
                subject_id, last_updated = None, player.get('updatedAt')
                subject, subject_team = player.get('name'), player.get('abvTeamName')
                if not subject_team:
                    subject_team = player.get('teamName')

                if subject:
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        cleaned_subject = DataCleaner.clean_subject(subject)
                        subject_id = self.dn.get_subject_id(cleaned_subject, league, subject_team)
                        subject_ids[f'{subject}{subject_team}'] = subject_id

                for prop in player.get('visiblePlayerProps', []):
                    market_id, market, line, multiplier = None, prop.get('p'), prop.get('val'), 1.0
                    if market:
                        market_id = self.dn.get_market_id(market)

                    mult_player_props = player.get('configPlayerProps')
                    if mult_player_props:
                        mult_market = mult_player_props.get(market)
                        if mult_market:
                            multiplier = mult_market.get('multiplier', multiplier)
                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'game_info': game_info,
                                'market_category': 'player_props',
                                'market_id': market_id,
                                'market': market,
                                'game_time': game_time,
                                'subject_team': subject_team,
                                'subject_id': subject_id,
                                'subject': subject,
                                'bookmaker': 'Vivid Picks',
                                'label': 'Over' if float(multiplier) > 1.0 else 'Under',
                                'line': line,
                                'multiplier': multiplier,
                            })

                    else:
                        for label in ['Over', 'Under']:
                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'last_updated': last_updated,
                                'league': league,
                                'game_info': game_info,
                                'market_category': 'player_props',
                                'market_id': market_id,
                                'market': market,
                                'game_time': game_time,
                                'subject_team': subject_team,
                                'subject_id': subject_id,
                                'subject': subject,
                                'bookmaker': 'Vivid Picks',
                                'label': label,
                                'line': line,
                                'multiplier': multiplier,
                            })

        self.helper.store(self.prop_lines)


async def main():
    db = get_db()
    spider = VividPicksSpider(uuid.uuid4(), RequestManager(), DataNormalizer('VividPicks', db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Vivid Picks]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
