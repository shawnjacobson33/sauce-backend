import asyncio
from collections import deque

from app import IN_SEASON_LEAGUES
from app import Subject, Market
from app import clean_market, clean_subject


# Champ formats leagues slightly differently...used for making requests
def get_in_season_leagues():
    league_name_map = {'NCAAF': 'CFB', 'NCAAB': 'CBB'}
    valid_champ_leagues = ['MLB', 'NFL', 'CFB', 'NBA', 'CBB', 'NHL']
    return [league_name_map.get(league, league) for league in IN_SEASON_LEAGUES if league_name_map.get(league, league) in valid_champ_leagues]


class Champ(ln_utils.LinesRetriever):
    def __init__(self, batch_id: str, bookmaker: ln_utils.LinesSource):
        super().__init__(batch_id, bookmaker)

    async def retrieve(self):
        url = ln_utils.get_url(self.name)
        headers = ln_utils.get_headers(self.name)
        tasks = []
        for league in get_in_season_leagues():
            if not is_league_good(clean_league(league)):
                continue

            json_data = ln_utils.get_json_data(self.name, var=league)
            tasks.append(self.req_mngr.post(url, self._parse_lines, league, headers=headers, json=json_data))

        await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league):
        # get body content in json format
        data = response.json() if response.json() else {}
        if data:
            data = data.get('data', {}).get('readPicks', {})

        league = dc_utils.clean_league(league)
        subject_ids = dict()
        for event in data.get('items', []):
            game_info = event.get('title')
            for player in event.get('competitors', []):
                subject_id, subject, position, subject_team, competitor = None, None, None, None, player.get('competitor')
                if competitor:
                    subject, team = competitor.get('longName'), competitor.get('team')
                    if team:
                        subject_team = team.get('shortName')
                        if subject:
                            subject = clean_subject(subject)
                            subject_id = subject_ids.get(f'{subject}{subject_team}')
                            if not subject_id:
                                subject_id = self.ds.get_subject(Subject(subject, league, subject_team))
                                subject_ids[f'{subject}{subject_team}'] = subject_id
                        else:
                            continue

                for prop in player.get('props', []):
                    market_id, market, line = None, prop.get('title'), prop.get('value')
                    if market :=
                        market = clean_market(market)
                        market_id = self.ds.get_market_id(Market(market, league))

                    labels, multiplier, boost = ['Over', 'Under'], None, prop.get('boost')
                    if boost:
                        multiplier = boost.get('multiplier')
                        labels = ['Under'] if multiplier < 1 else ['Over']

                    for label in labels:
                        # update shared data
                        PropLines.update(''.join(self.name.split()).lower(), {
                            'batch_ids': deque([self.batch_id]),
                            's_tstamp': str(datetime.now()),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market_id': market['id'],
                            'market': market['name'],
                            'subject_id': subject['id'],
                            'subject': subject['name'],
                            'bookmaker': self.name,
                            'label': label,
                            'line': line,
                            'multiplier': multiplier,
                            'odds': round(self.source.default_payout.odds * multiplier, 3) if multiplier else self.source.default_payout.odds
                        })
                        self.data_size += 1


if __name__ == "__main__":
    dc_utils.run(Champ))
