import asyncio
from datetime import datetime
from collections import defaultdict

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league



# TODO: Sleeper recently added alt_lines
class Sleeper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)
        # get universally used headers to make requests
        self.headers = self.req_packager.get_headers()

    async def start(self) -> None:
        # get the url required to request player data
        url = self.req_packager.get_url(name='players')
        # get params required to request player data
        params = self.req_packager.get_params()
        # make the request for player data
        await self.req_mngr.get(url, self._parse_players, headers=self.headers, params=params)

    async def _parse_players(self, response) -> None:
        # get the response data as json, if exists then keep executing
        if json_data := response.json():
            # initialize a dictionary to hold subject data
            players_dict = defaultdict(dict)
            # for each player's data in the json
            for player_data in json_data:
                # get the subject team and player id, if both exists then keep executing
                if subject_team := player_data.get('team') and (player_id := player_data.get('player_id')):
                    # get subject's first and last name, if both exist then keep executing
                    if (first_name := player_data.get('first_name')) and (last_name := player_data.get('last_name')):
                        # get the subject's sport, if exists then keep executing
                        if subject_sport := player_data.get('sport'):
                            # store data about the subject, corresponding to their sport and id
                            players_dict[subject_sport][player_id] = {
                                'subject_team': subject_team,
                                'subject': ' '.join([first_name, last_name]),
                                'position': player_data.get('position')
                            }

            # get the url required to make request for prop lines
            url = self.req_packager.get_url()
            # make the request for prop lines data
            await self.req_mngr.get(url, self._parse_lines, players_dict, headers=self.headers)

    # TODO: Left off here
    async def _parse_lines(self, response, players: dict) -> None:
        data = response.json()
        for line in data:
            subject_id, subject_team, subject, position, cleaned_league = None, None, None, None, None
            player_id, league = line.get('subject_id'), line.get('sport')
            if league:
                cleaned_league = clean_league(league)
                if not is_league_good(cleaned_league):
                    continue

            if player_id:
                player = players.get(league).get(player_id)
                if player:
                    subject_team, subject, position = player.get('subject_team'), player.get('player_name'), player.get(
                        'position')
                    if position:
                        position = clean_position(position)

                    if subject:
                        subject = clean_subject(subject)
                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                        if not subject_id:
                            subject_id = self.ds.get_subject_id(Subject(subject, cleaned_league, subject_team, position))
                            subject_ids[f'{subject}{subject_team}'] = subject_id

            market_id, last_updated, market = None, line.get('updated_at'), line.get('wager_type')
            if market:
                if market == 'fantasy_points':
                    market = FANTASY_SCORE_MAP.get(league, market)

                market = clean_market(market)
                market_id = self.ds.get_market_id(Market(market, cleaned_league))

            for option in line.get('options', []):
                label, line = option.get('outcome').title(), option.get('outcome_value')
                odds = option.get('payout_multiplier')
                # update shared data
                PropLines.update(''.join(self.info.name.split()).lower(), {
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': cleaned_league,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market': market,
                    'subject_id': subject_id,
                    'subject': subject,
                    'bookmaker': self.info.name,
                    'label': label,
                    'line': line,
                    'odds': odds
                })
                self.data_size += 1


if __name__ == "__main__":
    asyncio.run(run(Sleeper))
