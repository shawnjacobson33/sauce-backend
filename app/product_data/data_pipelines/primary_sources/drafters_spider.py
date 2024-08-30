import asyncio
import json
import os
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.request_management import AsyncRequestManager


class DraftersSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm

    async def start(self):
        url = 'https://node.drafters.com/props-game/get-props-games'
        headers = {
            'Host': 'node.drafters.com',
            'accept': '*/*',
            'app_version': '13.7',
            'if-none-match': 'W/"71e64-mEhxPh4ljtQEfs/ywHWFIsBrtwU"',
            'accept-language': 'en-US,en;q=0.9',
            'device_type': 'ios',
            'user-agent': 'Drafters/1 CFNetwork/1496.0.7 Darwin/23.5.0',
            'device_id': 'cwEL85ss0EYPrM04yxrkGf:APA91bG2MtKk_iLZ-_twLz8QU_6CscuU3DQkPVw9m-6mNPTssieqvi51X6MCBlVfGkx4ygTdi7vGCT2WZsGRe_GbY8sO4Scx34WxTErCaXBQwWZNMlDiZu9YODqBqv5MrnXBVN_nX9_f',
            'authorizations': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3MjQwOTkxMzUsImp0aSI6Ik5qQmxaalU1TXpRNFlqVTRObUkyWldVMk0yVTRNV1k0TVRVM1l6YzFOVE13WldZeVlqTXdNbUkzTkdObVpXTTNPREZsT1RRMU1EVTRaRGhtT0RNME1BPT0iLCJpc3MiOiJodHRwczpcL1wvYXBpLmRyYWZ0ZXJzLmNvbVwvIiwibmJmIjoxNzI0MDk5MTI1LCJleHAiOjE3NTUyMDMxMjUsImRhdGEiOnsiaWQiOiI4MTUwMyIsInVzZXJuYW1lIjoidGhlcmVhbHNsaW0iLCJkcmFmdGVyc19pZCI6InRoZXJlYWxzbGltIiwiZW1haWwiOiJzaGF3bmphY29ic29uMzNAZ21haWwuY29tIn19.p86Dtjv0L-mraFMhbcvHMdHparpUsR1tXEymQcdwMrg',
            'user_agent': 'iPhone 14 Pro',
            'access_token': 'draft_user',
        }

        await self.arm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()

        for event in data.get('entities', []):
            # do not want futures
            if 'Season' in event.get('_id'):
                continue

            for player in event.get('players', []):
                subject, position = player.get('player_name'), player.get('player_position')

                # get game info
                game_info, event = '', player.get('event')
                if event:
                    home_team, away_team = event.get('home'), event.get('away')
                    game_info = ' @ '.join([away_team, home_team])

                market, line = player.get('bid_stats_name'), player.get('bid_stats_value')

                for label in ['Over', 'Under']:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'market_category': 'player_props',
                        'market': market,
                        'game_info': game_info,
                        'subject': subject,
                        'position': position,
                        'bookmaker': 'Drafters',
                        'label': label,
                        'line': line
                    })

        relative_path = 'data_samples/drafters_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[Drafters]: {len(self.prop_lines)} lines')


async def main():
    spider = DraftersSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager())
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Drafters]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
