import asyncio
import sys
import time
import uuid
from datetime import datetime

from app.product_data.data_sourcing.utils import RequestManager, DataStandardizer, clean_market, clean_subject, \
    clean_league, Packager, get_db, Subject, Market, IN_SEASON_LEAGUES, Plug, Bookmaker, get_bookmaker


# Champ formats leagues slightly differently...used for making requests
def get_in_season_leagues():
    league_name_map = {'NCAAF': 'CFB', 'NCAAB': 'CBB'}
    valid_champ_leagues = ['MLB', 'NFL', 'CFB', 'NBA', 'CBB', 'NHL']
    return [league_name_map.get(league, league) for league in IN_SEASON_LEAGUES if league_name_map.get(league, league) in valid_champ_leagues]


class Champ(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []

    async def start(self):
        url = self.packager.get_url()
        headers = self.packager.get_headers()
        tasks = []
        for league in get_in_season_leagues():
            if not Packager.is_league_good(clean_league(league)):
                continue

            json_data = self.packager.get_json_data(var=league)
            tasks.append(self.rm.post(url, self._parse_lines, league, headers=headers, json=json_data))

        await asyncio.gather(*tasks)
        self.packager.store(self.prop_lines)

    async def _parse_lines(self, response, league):
        # get body content in json format
        data = response.json() if response.json() else {}
        if data:
            data = data.get('data', {}).get('readPicks', {})

        league = clean_league(league)
        subject_ids = dict()
        for event in data.get('items', []):
            game_info = event.get('title')
            for player in event.get('competitors', []):
                subject_id, subject, position, subject_team, competitor = None, None, None, None, player.get('competitor')
                if competitor:
                    subject, position = competitor.get('longName'), competitor.get('position')
                    team = competitor.get('team')
                    if team:
                        subject_team = team.get('shortName')
                        if subject:
                            subject = clean_subject(subject)
                            subject_id = subject_ids.get(f'{subject}{subject_team}')
                            if not subject_id:
                                subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team))
                                subject_ids[f'{subject}{subject_team}'] = subject_id
                        else:
                            continue

                for prop in player.get('props', []):
                    market_id, market, line = None, prop.get('title'), prop.get('value')
                    if market:
                        market = clean_market(market)
                        market_id = self.ds.get_market_id(Market(market, league))

                    labels, multiplier, boost = ['Over', 'Under'], None, prop.get('boost')
                    if boost:
                        multiplier = boost.get('multiplier')
                        labels = ['Under'] if multiplier < 1 else ['Over']

                    for label in labels:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market_id,
                            'market': market,
                            'subject_id': subject_id,
                            'subject': subject,
                            'bookmaker': self.info.name,
                            'label': label,
                            'line': line,
                            'multiplier': multiplier,
                            'odds': round(self.info.default_payout.odds * multiplier, 3) if multiplier else self.info.default_payout.odds
                        })


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    bookmaker_info = Bookmaker(get_bookmaker(db, "Champ"))
    spider = Champ(bookmaker_info, batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Champ]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    with open('log.txt', 'w') as f:
        sys.stdout = f
        asyncio.run(main())
