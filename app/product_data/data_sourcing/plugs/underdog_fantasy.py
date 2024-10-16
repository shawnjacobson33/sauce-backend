import asyncio
import time
import uuid
from datetime import datetime

from app.product_data.data_sourcing.utils import clean_subject, clean_league, clean_market, DataStandardizer, Packager, \
    RequestManager, get_db, Subject, Market, Plug, Bookmaker, get_bookmaker


class UnderdogFantasy(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []

    async def start(self):
        url = self.packager.get_url(name='teams')
        headers = self.packager.get_headers(name='teams')
        cookies = self.packager.get_cookies()
        await self.rm.get(url, self._parse_teams, headers=headers, cookies=cookies)

    async def _parse_teams(self, response):
        data = response.json()
        teams = dict()
        for team in data.get('teams'):
            team_id, team_abbr = team.get('id'), team.get('abbr')
            if team_id and team_abbr:
                teams[team_id] = team_abbr

        url = self.packager.get_url()
        headers = self.packager.get_headers()
        await self.rm.get(url, self._parse_lines, teams, headers=headers)

    async def _parse_lines(self, response, teams):
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
            subject_team, player_id, team_id = None, player.get('id'), player.get('team_id')
            if team_id:
                subject_team = teams.get(team_id)

            first_name, last_name = player.get('first_name'), player.get('last_name')
            if not first_name:
                subject = last_name
            elif not last_name:
                subject = first_name
            else:
                subject = ' '.join([first_name, last_name])

            if player_id:
                player_data[player_id] = {'subject': subject, 'subject_team': subject_team}

        subject_ids = dict()
        leagues = set()
        for game in data.get('over_under_lines', []):
            line = game.get('stat_value')
            for option in game.get('options', []):
                # retrieve the match data corresponding to the appearance id of this line
                subject_id, market_id, subject_team = None, None, None
                league, market, subject, game_time, over_under = None, None, None, None, game.get('over_under')
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
                                if ('SZN' in league) or ('COMBOS' in league) or not Packager.is_league_good(clean_league(league)):
                                    continue

                                if league:
                                    league = clean_league(league)
                                    leagues.add(league)

                        # get market and market id
                        if market:
                            # create more distinct markets
                            if market == 'Fantasy Points':
                                if league in {'NBA', 'WNBA'}:
                                    market = 'Basketball Fantasy Points'
                                elif league == 'MLB':
                                    market = 'Baseball Fantasy Points'
                                elif league in {'NFL', 'NCAAF'}:
                                    market = 'Football Fantasy Points'

                            market = clean_market(market)
                            market_id = self.ds.get_market_id(Market(market, league))

                        # get subject
                        player_id = player_ids.get(appearance_id)
                        if player_id:
                            player = player_data.get(player_id)
                            if player:
                                subject, subject_team = player.get('subject'), player.get('subject_team')

                            # fixes a formatting issues for ESPORTS subjects where Underdog formats like this:
                            # CS: [subject_name] and I only want the subject
                            if league == 'ESPORTS':
                                subject_components = subject.split(': ')
                                if subject_components and len(subject_components) == 2:
                                    # assign the league to the more specific game in the ESPORTS realm
                                    league, subject = subject_components[0], subject_components[1]

                            # subjects show up more than once so don't need to get subject id every time.
                            subject = clean_subject(subject)
                            subject_id = subject_ids.get(f'{subject}{subject_team}')
                            if subject:
                                subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team))
                                subject_ids[f'{subject}{subject_team}'] = subject_id

                label = 'Over' if option.get('choice') == 'higher' else 'Under'
                multiplier = option.get('payout_multiplier')
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
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

        self.packager.store(self.prop_lines)
        print(leagues)


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    bookmaker_info = Bookmaker(get_bookmaker(db, "Underdog Fantasy"))
    spider = UnderdogFantasy(bookmaker_info, batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[Underdog Fantasy]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
