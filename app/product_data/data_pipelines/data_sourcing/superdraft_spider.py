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


class SuperDraftSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, db: Database):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm, self.msc, self.plc = arm, db['markets'], db['prop_lines']

    async def start(self):
        url = 'https://api.superdraft.io/api/prop/v3/active-fantasy'
        headers = {
            'Host': 'api.superdraft.io',
            'content-type': 'application/json',
            'accept': 'application/json',
            'baggage': 'sentry-environment=production,sentry-public_key=a1c7747ba00849cab409e4e842041a1c,sentry-release=dfs-mobile%401.7.93%20%287%29,sentry-trace_id=c33fcdef6fa742f8966c3a4c780c53cd',
            'timestamp': '2024-08-19T19:19:44.652Z',
            'api-key': 'cont*177',
            'device-type': '1',
            'session-key': '6fb93f6f2c83a5091724081704',
            'product-type': 'sd-dfs',
            'sentry-trace': 'c33fcdef6fa742f8966c3a4c780c53cd-d5e48157b681463a-0',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'SuperDraft/7 CFNetwork/1496.0.7 Darwin/23.5.0',
            'datatype': 'json',
            'attribution': '{"idfv":"6FB6DCDD-6CC1-46A7-B206-8AFAE050CE89","android_id":"","idfa":"00000000-0000-0000-0000-000000000000","gps_adid":"","messageId":"","templateId":"","campaignId":""}',
            'geo-token': 'geoc84ee9df27b0e99a1724081704',
        }

        await self.arm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()

        # get sports ids
        sports = dict()
        for sport in data.get('sports', []):
            sport_id, sport_name = sport.get('sportId'), sport.get('sName')
            if sport_id and sport_name:
                sports[sport_id] = sport_name

        # get props
        for prop in data.get('props', []):
            # not doing matchup props
            prop_type = prop.get('type')
            if prop_type != 'matchup-prop':
                last_updated, game_time = prop.get('updatedAt'), prop.get('startTimeUTC')
                league = sports.get(int(prop.get('sportId')))
                if league:
                    league = dc.clean_league(league)

                # get market
                market_id, market, choices = '', '', prop.get('choices')
                if choices:
                    actor = choices[0].get('actor')
                    if actor:
                        requirements = actor.get('winningRequirement')
                        if requirements:
                            market = requirements[0].get('name')
                            if market in {'Fantasy Hitting', 'Fantasy Pitching'}:
                                market = 'Baseball Fantasy Score'
                            market_id = self.msc.find_one({'SuperDraft': market}, {'_id': 1})
                            if market_id:
                                market_id = market_id.get('_id')

                # get player data
                player = prop.get('player')
                player_first_name, player_last_name = player.get('fName'), player.get('lName')
                position, team, subject = '', '', ' '.join([player_first_name, player_last_name])
                if player_first_name == 'combined':
                    players, teams, positions = [], [], []
                    for player in prop.get('players', []):
                        player_first_name = player.get('fName')
                        player_last_name = player.get('lName')

                        positions.append(player.get('posAbbr'))
                        teams.append(player.get('teamAbbr'))
                        players.append(' '.join([player_first_name, player_last_name]))

                    subject = ' + '.join(players)
                else:
                    team, position = player.get('teamAbbr'), player.get('posAbbr')

                game_info, line = player.get('eventName'), prop.get('line')

                for label in ['Over', 'Under']:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'last_updated': last_updated,
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market_name': market,
                        'game_info': game_info,
                        'game_time': game_time,
                        'subject_team': team,
                        'subject': subject,
                        'position': position,
                        'bookmaker': 'SuperDraft',
                        'label': label,
                        'line': line
                    })

        relative_path = '../data_samples/superdraft_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        # self.plc.insert_many(self.prop_lines)

        print(f'[SuperDraft]: {len(self.prop_lines)} lines')


async def main():
    client = MongoClient('mongodb://localhost:27017/', uuidRepresentation='standard')

    db = client['sauce']

    spider = SuperDraftSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), db=db)
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[SuperDraft]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
