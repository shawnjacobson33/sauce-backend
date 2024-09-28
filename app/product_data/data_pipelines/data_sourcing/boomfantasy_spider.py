import asyncio
import os
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.utils import RequestManager, DataStandardizer, Helper, clean_market, clean_subject, \
    clean_league, get_db, Subject, Market


def read_tokens():
    absolute_path = os.path.abspath('boomfantasy_tokens.txt')
    with open(absolute_path, 'r') as file:
        access_token, refresh_token = [line.strip() for line in file.readlines()[:2]]
    return {'path': absolute_path, 'access_token': access_token, 'refresh_token': refresh_token}


class BoomFantasySpider:
    def __init__(self, batch_id: str, request_manager: RequestManager, data_standardizer: DataStandardizer):
        self.batch_id = batch_id
        self.helper = Helper(bookmaker='BoomFantasy')
        self.rm = request_manager
        self.ds = data_standardizer
        self.prop_lines = []

    async def start(self):
        url = self.helper.get_url()
        json_data = self.helper.get_json_data()
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
                            if not Helper.is_league_good(league_name):
                                continue

                        for league_section in league.get('sections', []):
                            game_time = league_section.get('lockTime')
                            if game_time:
                                # convert from unix to a datetime
                                game_time = datetime.fromtimestamp(game_time / 1000)

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

                                    self.prop_lines.append({
                                        'batch_id': self.batch_id,
                                        'time_processed': datetime.now(),
                                        'league': league_name,
                                        'market_category': 'player_props',
                                        'market_id': market_id,
                                        'market': market,
                                        'game_time': game_time,
                                        'subject_id': subject_id,
                                        'subject': subject,
                                        'bookmaker': 'BoomFantasy',
                                        'label': label,
                                        'line': line
                                    })

            self.helper.store(self.prop_lines)


async def main():
    db = get_db()
    batch_id = str(uuid.uuid4())
    with open('most_recent_batch_id.txt', 'w') as f:
        f.write(batch_id)

    print(f'Batch ID: {batch_id}')
    spider = BoomFantasySpider(batch_id, RequestManager(), DataStandardizer(batch_id, db))
    start_time = time.time()
    await spider.start()
    end_time = time.time()
    print(f'[BoomFantasy]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
