import asyncio
from datetime import datetime

from app.product_data.data_collection.shared_data import PropLines
from app.product_data.data_collection.utils.constants import FANTASY_SCORE_MAP
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.helpers.misc import run, is_league_good
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class SuperDraft(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, req_mngr, data_standardizer)

    async def start(self):
        url = self.req_packager.get_url()
        headers = self.req_packager.get_headers()
        await self.req_mngr.get(url, self._parse_lines, headers=headers)

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
                league = sports.get(int(prop.get('sportId')))
                if league:
                    if not is_league_good(league):
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
                            if market:
                                if 'Fantasy' in market:
                                    market = FANTASY_SCORE_MAP.get(league, market)

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
                    if position:
                        position = clean_position(position)

                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                            subject_ids[f'{subject}{subject_team}'] = subject_id

                game_info, line = player.get('eventName'), prop.get('line')
                for label in ['Over', 'Under']:
                    # update shared data
                    PropLines.update(''.join(self.info.name.split()).lower(), {
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now(),
                        'league': league,
                        'market_category': 'player_props',
                        'market_id': market_id,
                        'market': market,
                        'game_info': game_info,
                        'subject_id': subject_id,
                        'subject': subject,
                        'bookmaker': self.info.name,
                        'label': label,
                        'line': line,
                        'odds': self.info.default_payout.odds
                    })
                    self.data_size += 1


if __name__ == "__main__":
    asyncio.run(run(SuperDraft))
