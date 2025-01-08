import asyncio

from bs4 import BeautifulSoup

from app.db import db
from app.services.utils import utilities as utils


PAYLOAD = utils.requester.get_payload('teams', 'CBSSports')


async def _request_rosters(collected_rosters: list, league: str, team: dict) -> None:
    base_url = PAYLOAD['urls'][league]['rosters']
    headers = PAYLOAD['headers']
    cookies = PAYLOAD['cookies']
    url = base_url.format(team['abbr_name'], team['full_name'])
    if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
        _parse_rosters(collected_rosters, league, team, resp_html)


def _parse_rosters(collected_rosters: list, league: str, team: dict, html: str) -> None:
    soup = BeautifulSoup(html, 'html.parser')
    if table := soup.find('table'):
        if (rows := table.find_all('tr')) and (len(rows) > 1):
            for row in rows[1:]:
                if (cells := row.find_all('td')) and (len(cells) > 2):
                    if span_elem := cells[1].find('span', {'class': 'CellPlayerName--long'}):
                        if a_elem := span_elem.find('a'):
                            collected_rosters.append({
                                'league': league,
                                'team': team,
                                'position': cells[2].text.strip(),
                                'subject': a_elem.text.strip(),
                                'jersey_number': cells[0].text.strip(),
                            })


async def run_cbssports_basketball_rosters_collector(collected_rosters: list, teams: dict = None):
    tasks = []
    for league in PAYLOAD['leagues_to_collect_from']:
        if not teams:
            teams = await db.teams.get_teams({'league': league})

        for team in teams:
            tasks.append(_request_rosters(collected_rosters, league, team))

    await asyncio.gather(*tasks)
    print(collected_rosters)


if __name__ == '__main__':
    asyncio.run(run_cbssports_basketball_rosters_collector([]))
