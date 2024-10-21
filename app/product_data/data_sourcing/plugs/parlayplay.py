import asyncio
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.constants import FANTASY_SCORE_MAP
from app.product_data.data_sourcing.utils.network_management import RequestManager, Packager
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league, clean_position


class ParlayPlay(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url()
        headers = self.packager.get_headers()
        await self.rm.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response):
        data = response.json()
        subject_ids = dict()
        for player in data.get('players', []):
            league, match = None, player.get('match')
            if match:
                player_league = match.get('league')
                if player_league:
                    league = player_league.get('leagueNameShort')
                    if league:
                        league = clean_league(league)
                        if not Packager.is_league_good(league):
                            continue

            # get subject data
            subject_id, subject, position, subject_team, player_data = None, None, None, None, player.get('player')
            if player_data:
                subject, position = player_data.get('fullName'), player_data.get('position')
                if position:
                    position = clean_position(position)
                    
                team_data = player_data.get('team')
                if team_data:
                    subject_team = team_data.get('teamAbbreviation')

                subject = clean_subject(subject)
                subject_id = subject_ids.get(f'{subject}{subject_team}')
                if not subject_id:
                    subject_id = self.ds.get_subject_id(Subject(subject, league, subject_team, position))
                    subject_ids[f'{subject}{subject_team}'] = subject_id

            for stat in player.get('stats', []):
                is_boosted_payout, alt_lines = stat.get('isBoostedPayout'), stat.get('altLines')
                if alt_lines:
                    market, market_id = alt_lines.get('market'), None
                    if market in {'Player Fantasy Score', 'Fantasy Points'}:
                        market = FANTASY_SCORE_MAP.get(league, market)

                    if market:
                        market = clean_market(market)
                        market_id = self.ds.get_market_id(Market(market, league))

                    for line in alt_lines.get('values', []):
                        stat_line = line.get('selectionPoints')
                        for label in ['Over', 'Under']:
                            odds = line.get('decimalPriceOver') if label == 'Over' else line.get('decimalPriceUnder')
                            if not odds:
                                continue

                            # update shared data
                            PropLines.update(''.join(self.info.name.split()).lower(), {
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
                                'line': stat_line,
                                'odds': odds,
                                'is_boosted': is_boosted_payout
                            })
                            self.data_size += 1


if __name__ == "__main__":
    import app.product_data.data_sourcing.plugs.helpers.helpers as helper
    asyncio.run(helper.run(ParlayPlay))
