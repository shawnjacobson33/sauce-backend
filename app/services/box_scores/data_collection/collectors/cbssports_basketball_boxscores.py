import asyncio
from typing import Iterable

from bs4 import BeautifulSoup

from app.services.configs import load_configs
from app.services.utils import utilities as utils, Standardizer

CONFIGS = load_configs('general')
PAYLOAD = utils.requester.get_payload('CBSSports', domain='boxscores')


class CBSSportsBasketballBoxscoresCollector:

    def __init__(self, standardizer: Standardizer):
        self.standardizer = standardizer

    async def _request_boxscores(self, collected_boxscores: list, league: str, game: dict):
        game_id = game['id']
        base_url = PAYLOAD['url'].format(league, game_id)
        headers = PAYLOAD['headers']
        # Todo: need to update referer
        cookies = PAYLOAD['cookies']
        url = base_url.format(game_id)
        if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
            self._parse_boxscores(collected_boxscores, resp_html, league, game)

    @staticmethod
    def _extract_team(div) -> dict[str, str] | None:
        if (a_elem := div.find('a')) and (href := a_elem.get('href')):
            if (len(href) > 3) and (team_name := href.split('/')[3]):
                return team_name

    @staticmethod
    def _extract_position(span_elem) -> str:
        position = span_elem.text.strip()
        return position

    def _extract_subject(self, cell, league: str) -> dict[str, str] | None:
        try:
            if (a_elem := cell.find('a')) and (href := a_elem.get('href')):
                raw_subject_name = ' '.join(href.split('/')[-2].split('-')).title()
                cleaned_subject_name = utils.cleaner.clean_subject_name(raw_subject_name)
                subject_key = utils.storer.get_subject_key(league, cleaned_subject_name)
                std_subject_name = self.standardizer.standardize_subject_name(subject_key)
                return {
                    'id': subject_key,
                    'name': std_subject_name,
                }

        except Exception as e:
            print(f'[BoxScores] [Collection]: ⚠️', e, '⚠️')

    @staticmethod
    def _extract_basketball_stats(cells, league: str) -> dict | None:
        data = [cell.text for cell in cells if 'for-mobile' not in cell.get('class')]
        minutes_played = data[7]
        if (minutes_played != '-') and (int(minutes_played) > 1):
            box_score = {
                'Points': int(data[0]),
                'Rebounds': int(data[1]),
                'Assists': int(data[2]),
                'Field Goals Made': int(data[3].split('/')[0]),
                'Field Goals Attempted': int(data[3].split('/')[1]),
                '3-Pointers Made': int(data[4].split('/')[0]),
                '3-Pointers Attempted': int(data[4].split('/')[1]),
                'Free Throws Made': int(data[5].split('/')[0]),
                'Free Throws Attempted': int(data[5].split('/')[1]),
                'Personal Fouls': int(data[6]),
                'Minutes Played': int(minutes_played),
                'Steals': int(data[8]),
                'Blocks': int(data[9]),
                'Turnovers': int(data[10]),
            }

            if league == 'NBA':
                # college basketball doesn't have these stats on cbssports
                box_score['Plus Minus'] = int(data[11][1:]) if '+' in data[11] else int(data[11])
                box_score['Fantasy Points'] = int(data[12])

            return box_score

    @staticmethod
    def _get_divs(soup: BeautifulSoup) -> Iterable:
        for team_name_div in soup.find_all('div', {'class': 'team-name'}):
            for box_score_div in soup.find_all('div', class_=['starters-stats', 'bench-stats']):
                yield team_name_div, box_score_div

    @staticmethod
    def _get_table(box_score_div):
        if table_div := box_score_div.find('div', {'class': 'stats-viewable-area'}):
            if table := table_div.find('table', {'class': 'stats-table'}):
                return table

    def _get_rows(self, box_score_div):
        if table := self._get_table(box_score_div):
            if (rows := table.find_all('tr')) and len(rows) > 1:
                for row in [row for row in rows if row.get('class')[0] not in {'header-row', 'total-row'}]:
                    yield row


    def _get_cells(self, box_score_div):
        for row in self._get_rows(box_score_div):
            if cells := row.find_all('td'):
                cells = [cell for cell in cells if 'for-mobile' not in cell.get('class')]
                return cells


    def _parse_boxscores(self, collected_boxscores: list, html: str, league: str, game: dict) -> None:
        soup = BeautifulSoup(html, 'html.parser')
        for team_name_div, box_score_div in self._get_divs(soup):
            if cells := self._get_cells(box_score_div):
                if subject := self._extract_subject(cells[0], league):
                    if box_score := self._extract_basketball_stats(cells[1:], league):
                        collected_boxscores.append({
                            '_id': f'{game['id']}:{subject['id']}',
                            'game': game,
                            'league': league,
                            'subject': subject,
                            'box_score': box_score
                        })

    async def run_collector(self, collected_boxscores: list, games: list[dict]):
        tasks = []
        for league in CONFIGS['leagues_to_collect_from']:
            if utils.get_sport(league) == 'Basketball':
                for game in games[league]:
                    tasks.append(self._request_boxscores(collected_boxscores, league, game))

        await asyncio.gather(*tasks)
