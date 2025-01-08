import asyncio

from bs4 import BeautifulSoup

from app.services.utils import utilities as utils
from app.services.teams.data_collection.configs import CONFIGS


PAYLOAD = utils.requester.get_payload('teams', 'CBSSports')


async def _request_teams(collected_team_names: list, league: str) -> dict | None:
    url = PAYLOAD['urls'][league]['teams']
    headers = PAYLOAD['headers']
    cookies = PAYLOAD['cookies']
    if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
        return _parse_teams(collected_team_names, league, resp_html)


def _parse_teams(collected_team_names: list, league: str, html: str) -> None:
    soup = BeautifulSoup(html, 'html.parser')
    if tables := soup.find_all('table'):
        if conference_names := soup.find_all('span', {'class': 'TeamLogoNameLockup-name'}):
            if len(tables) == len(conference_names):
                for table, conference_name in zip(tables, conference_names):
                    if (league != 'NCAA') or (conference_name.text.strip() in CONFIGS['ncaa_conferences_to_collect_from']):
                        if (rows := table.find_all('tr')) and (len(rows) > 1):
                            for row in rows[1:]:
                                if row.get('class') != 'TableBase-headTr':
                                    if team_name_span := row.find('span', {'class': 'TeamName'}):
                                        if (a_elem := team_name_span.find('a')) and (href := a_elem.get('href')):
                                            href_comps = href.split("/")
                                            if len(href_comps) > 3:
                                                abbr_name, full_name = href_comps[3], a_elem.text
                                                collected_team_names.append({
                                                    'league': league,
                                                    'abbr_name': abbr_name,
                                                    'full_name': full_name,
                                                })

async def run_cbssports_team_names_collector():
    collected_team_names = []
    tasks = []
    for league in CONFIGS['leagues_to_collect_from']:
        tasks.append(_request_teams(collected_team_names, league))

    await asyncio.gather(*tasks)
    print(collected_team_names)
    return collected_team_names


if __name__ == '__main__':
    asyncio.run(run_cbssports_team_names_collector())



