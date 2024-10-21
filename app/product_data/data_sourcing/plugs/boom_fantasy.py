import asyncio
import os
from datetime import datetime

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils.network_management import RequestManager
from app.product_data.data_sourcing.plugs.helpers.helpers import run, is_league_good
from app.product_data.data_sourcing.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_sourcing.utils.data_wrangling import DataStandardizer, clean_market, clean_subject, \
    clean_league


def read_tokens():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.join(script_dir, 'tokens', 'boomfantasy_tokens.txt')
    with open(absolute_path, 'r') as file:
        access_token, refresh_token = [line.strip() for line in file.readlines()[:2]]
    return {'path': absolute_path, 'access_token': access_token, 'refresh_token': refresh_token}


class BoomFantasy(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        super().__init__(info, batch_id, request_manager, data_standardizer)

    async def start(self):
        url = self.packager.get_url()
        json_data = self.packager.get_json_data()
        tokens = read_tokens()
        await self.rm.post_bf(url, self._parse_lines, tokens['path'], tokens['refresh_token'], tokens['access_token'],
                              json_data=json_data)

    async def _parse_lines(self, response):
        data = response.json().get('data')
        contest = data.get('contest')
        if contest:
            subject_ids = dict()
            for section in contest.get('sections', []):
                selection_type = section.get('type')
                # don't want the 'matchups' data
                if selection_type == 'overUnder':
                    for league in section.get('leagues', []):
                        league_name = league.get('league')
                        if league_name:
                            league_name = clean_league(league_name.upper())
                            if not is_league_good(league_name):
                                continue

                        for league_section in league.get('sections', []):
                            # get team
                            subject_team, player_image = None, league_section.get('playerImage')
                            if player_image:
                                subject_team = player_image.get('abbreviation')

                            # get subject
                            subject_id, subject, title = None, None, league_section.get('title')
                            if title:
                                options = title.get('additionalOptions')
                                if options:
                                    first_name, last_name = options.get('firstName'), options.get('lastName')
                                    subject = ' '.join([first_name, last_name])
                                    if subject:
                                        # subjects show up more than once so don't need to get subject id every time.
                                        subject = clean_subject(subject)
                                        subject_id = subject_ids.get(f'{subject}{subject_team}')
                                        if not subject_id:
                                            subject_id = self.ds.get_subject_id(Subject(subject, league_name, subject_team))
                                            subject_ids[f'{subject}{subject_team}'] = subject_id

                            for question in league_section.get('fullQuestions', []):
                                # get line and market
                                line, market, market_id = None, None, None
                                pick_selection_title = question.get('pickSelectionTitle')
                                if pick_selection_title:
                                    additional_options = pick_selection_title.get('additionalOptions')
                                    if additional_options:
                                        line = additional_options.get('line')
                                        stat_text = additional_options.get('statText')
                                        if stat_text:
                                            stat_text_components = stat_text[0].split('.')
                                            if len(stat_text_components) == 4:
                                                market = stat_text_components[-2].lower().title()
                                                if market:
                                                    market = clean_market(market)
                                                    market_id = self.ds.get_market_id(Market(market, league_name))

                                for choice in question.get('choices', []):
                                    label = choice.get('type')
                                    if label:
                                        label = label.title()

                                    # update shared data
                                    PropLines.update(''.join(self.info.name.split()).lower(), {
                                        'batch_id': self.batch_id,
                                        'time_processed': datetime.now(),
                                        'league': league_name,
                                        'market_category': 'player_props',
                                        'market_id': market_id,
                                        'market': market,
                                        'subject_id': subject_id,
                                        'subject': subject,
                                        'bookmaker': self.info.name,
                                        'label': label,
                                        'line': line,
                                        'odds': self.info.default_payout.odds
                                    })
                                    self.data_size += 1


if __name__ == "__main__":
    asyncio.run(run(BoomFantasy))
