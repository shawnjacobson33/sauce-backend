import asyncio

from bs4 import BeautifulSoup

from app.db import db
from app.services.configs import load_configs
from app.services.utils import utilities as utils

CONFIGS = load_configs('general')
PAYLOAD = utils.requester.get_payload('CBSSports', domain='teams')


async def _request_rosters(collected_rosters: list, league: str, team: dict) -> None:
    base_url = PAYLOAD['urls'][league]['rosters']
    headers = PAYLOAD['headers']
    headers['referer'] = headers['referer'].format(league, 'teams')
    cookies = PAYLOAD['cookies']
    abbr_team_name = team['abbr_name']
    full_team_name = ('-'.join(team['full_name'].lower().split())
                      .replace('.', '')
                      .replace('(', '')
                      .replace(')', '')
                      .replace("'", ''))
    if 'NCAA' in league:
        abbr_team_name = PAYLOAD['ncaa_team_name_url_map'][league]['abbr_name'].get(abbr_team_name, abbr_team_name)
        full_team_name = PAYLOAD['ncaa_team_name_url_map'][league]['full_name'].get(full_team_name, full_team_name)

    url = base_url.format(abbr_team_name, full_team_name)
    if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
        _parse_rosters(collected_rosters, league, team, resp_html)


def _parse_rosters(collected_rosters: list, league: str, team: dict, html: str) -> None:
    roster = { 'league': league, 'team': team, 'players': [] }
    soup = BeautifulSoup(html, 'html.parser')
    if table := soup.find('table'):
        if (rows := table.find_all('tr')) and (len(rows) > 1):
            for row in rows[1:]:
                if (cells := row.find_all('td')) and (len(cells) > 2):
                    if span_elem := cells[1].find('span', {'class': 'CellPlayerName--long'}):
                        if a_elem := span_elem.find('a'):
                            roster['players'].append({
                                'position': cells[2].text.strip(),
                                'name': a_elem.text.strip(),
                                'jersey_number': cells[0].text.strip(),
                            })

            collected_rosters.append(roster)


async def run_collector(collected_rosters: list):
    tasks = []
    for league in CONFIGS['leagues_to_collect_from']:
        if utils.get_sport(league) == 'Basketball':
            teams = await db.teams.get_teams({ 'league': league if 'NCAA' not in league else 'NCAA' })  # Todo: fine for now...optimize in the future
            for team in teams:
                tasks.append(_request_rosters(collected_rosters, league, team))

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(run_collector([]))
