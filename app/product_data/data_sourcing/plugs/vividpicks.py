import asyncio
import main
from datetime import datetime

from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_manipulation import DataStandardizer, clean_market, clean_subject, \
    clean_league


class VividPicks(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)
        self.prop_lines = []

    async def start(self):
        url = self.packager.get_url()
        headers = self.packager.get_headers()
        json_data = self.packager.get_json_data()
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
                league = clean_league(league)
                if not Packager.is_league_good(league):
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
                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                # 'last_updated': last_updated,
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
                            self.prop_lines.append({
                                'batch_id': self.batch_id,
                                'time_processed': datetime.now(),
                                # 'last_updated': last_updated,
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

        self.packager.store(self.prop_lines)


if __name__ == "__main__":
    asyncio.run(main.run(VividPicks))
