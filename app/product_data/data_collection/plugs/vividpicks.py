import asyncio
from datetime import datetime

from app.product_data.data_collection.shared_data import PropLines
from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.plugs.helpers.misc import run, is_league_good
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import DataStandardizer, clean_market, clean_subject, \
    clean_league


class VividPicks(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, req_mngr, data_standardizer)

    async def start(self):
        url = self.req_packager.get_url()
        headers = self.req_packager.get_headers()
        json_data = self.req_packager.get_json_data()
        await self.req_mngr.post(url, self._parse_lines, headers=headers, json=json_data)

    async def _parse_lines(self, response):
        # get body content in json format
        data = response.json()
        subject_ids = dict()
        for event in data.get('gret', []):
            league, game_info, game_time = event.get('league'), event.get('gameInfo'), event.get('gameTime')
            if 'Futures' in game_info:
                continue

            if league:
                league = clean_league(league)
                if not is_league_good(league):
                    continue

            for player in event.get('activePlayers', []):
                subject_id, last_updated = None, player.get('updatedAt')
                subject, subject_team = player.get('name'), player.get('abvTeamName')
                if not subject_team:
                    subject_team = player.get('teamName')

                if subject:
                    subject = clean_subject(subject)
                    subject_id = subject_ids.get(f'{subject}{subject_team}')
                    if not subject_id:
                        subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team))
                        subject_ids[f'{subject}{subject_team}'] = subject_id

                for prop in player.get('visiblePlayerProps', []):
                    market_id, market, line, multiplier = None, prop.get('p'), prop.get('val'), 1.0
                    if market:
                        market = clean_market(market)
                        market_id = self.ds.get_market_id(Market(market, league))

                    mult_player_props = player.get('configPlayerProps')
                    if mult_player_props:
                        mult_market = mult_player_props.get(market)
                        if mult_market:
                            multiplier = mult_market.get('multiplier', multiplier)
                            PropLines.update(''.join(self.info.name.split()).lower(), {
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
                                'label': 'Over' if float(multiplier) > 1.0 else 'Under',
                                'line': line,
                                'multiplier': multiplier,
                                'odds': round(self.info.default_payout.odds * multiplier, 3) if multiplier else self.info.default_payout.odds
                            })

                    else:
                        for label in ['Over', 'Under']:
                            # update shared data
                            PropLines.update(''.join(self.info.name.split()).lower(), {
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                'league': league,
                                'game_info': game_info,
                                'market_category': 'player_props',
                                'market_id': market_id,
                                'market': market,
                                'subject': subject,
                                'bookmaker': self.info.name,
                                'label': label,
                                'line': line,
                                'multiplier': multiplier,
                                'odds': round(self.info.default_payout.odds * multiplier,
                                              3) if multiplier else self.info.default_payout.odds
                            })

                    self.data_size += 1


if __name__ == "__main__":
    asyncio.run(run(VividPicks))
