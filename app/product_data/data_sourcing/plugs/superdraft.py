import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_sourcing.utils import clean_market, clean_league, clean_subject, RequestManager, \
    DataStandardizer, Packager, get_db, Subject, Market


class SuperDraftPlug:
    def __init__(self, batch_id: uuid.UUID, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.packager = Packager(bookmaker='SuperDraft')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []

    async def start(self):
        url = self.packager.get_url()
        headers = self.packager.get_headers()
        await self.rm.get(url, self._parse_lines, headers=headers)

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
        subject_ids = dict()
        for prop in data.get('props', []):
            # not doing matchup props
            prop_type = prop.get('type')
            if prop_type != 'matchup-prop':
                last_updated, game_time = prop.get('updatedAt'), prop.get('startTimeUTC')
                league = sports.get(int(prop.get('sportId')))
                if league:
                    if not Packager.is_league_good(league):
                        continue

                    league = clean_league(league)

                # get market
                market_id, market, choices = None, None, prop.get('choices')
                if choices:
                    actor = choices[0].get('actor')
                    if actor:
                        requirements = actor.get('winningRequirement')
                        if requirements:
                            market = requirements[0].get('name')
                            if market in {'Fantasy Hitting', 'Fantasy Pitching'}:
                                market = 'Baseball Fantasy Points'
                            if market:
                                market = clean_market(market)
                                market_id = self.ds.get_market_id(Market(market, league))

                # get player data
                subject_id, player = None, prop.get('player')
                player_first_name, player_last_name = player.get('fName'), player.get('lName')
                position, subject_team, subject = None, None, ' '.join([player_first_name, player_last_name])
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
                    subject_team, position = player.get('teamAbbr'), player.get('posAbbr')
                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                            subject_ids[f'{subject}{subject_team}'] = subject_id

                game_info, line = player.get('eventName'), prop.get('line')
                for label in ['Over', 'Under']:
                    self.prop_lines.append({
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'last_updated': last_updated,
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'game_info': game_info,
                        'game_time': game_time,
                        'subject_team': subject_team,
                        'position': position,
                        'subject_id': subject_id,
                        'subject': subject,
                        'bookmaker': 'SuperDraft',
                        'label': label,
                        'line': line
                    })

        self.packager.store(self.prop_lines)


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = SuperDraftPlug(uuid.uuid4(), RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[SuperDraft]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
