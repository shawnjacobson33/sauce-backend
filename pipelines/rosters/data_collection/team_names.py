# import asyncio
#
# from bs4 import BeautifulSoup
#
# from app.pipelines.configs import load_configs
# from app.pipelines.utils import utilities as utils
#
#
# CONFIGS = load_configs('general')
# PAYLOAD = utils.requester.get_payload('CBSSports', domain='teams')
#
#
# async def _request_teams(collected_teams: list, league: str) -> dict | None:
#     url = PAYLOAD['urls'][league]['teams']
#     headers = PAYLOAD['headers']
#     headers['referer'] = headers['referer'].format(league, 'teams')
#     cookies = PAYLOAD['cookies']
#     if resp_html := await utils.requester.fetch(url, to_html=True, headers=headers, cookies=cookies):
#         return _parse_teams(collected_teams, league, resp_html)
#
#
# def _parse_teams(collected_teams: list, league: str, html: str) -> None:
#     soup = BeautifulSoup(html, 'html.parser')
#     if tables := soup.find_all('table'):
#         if conference_names := soup.find_all('span', {'class': 'TeamLogoNameLockup-name'}):
#             if len(tables) == len(conference_names):
#                 for table, conference_name in zip(tables, conference_names):
#                     if (league != 'NCAA') or (conference_name.text.strip() in CONFIGS['ncaa_conferences_to_collect_from']):
#                         if (rows := table.find_all('tr')) and (len(rows) > 1):
#                             for row in rows[1:]:
#                                 if row.get('class') != 'TableBase-headTr':
#                                     if team_name_span := row.find('span', {'class': 'TeamName'}):
#                                         if (a_elem := team_name_span.find('a')) and (href := a_elem.get('href')):
#                                             href_comps = href.split("/")
#                                             if len(href_comps) > 3:
#                                                 abbr_name, full_name = href_comps[3], a_elem.text
#                                                 collected_teams.append({
#                                                     'league': league,
#                                                     'abbr_name': abbr_name,
#                                                     'full_name': full_name,
#                                                 })
#
# async def run_cbssports_team_names_collector() -> None:
#     from app.db import db
#     collected_teams = []
#     tasks = []
#     for league in CONFIGS['valid_leagues']:
#         tasks.append(_request_teams(collected_teams, league))
#
#     await asyncio.gather(*tasks)
#     print(collected_teams)
#     await db.teams.store_teams(collected_teams)
#
#
# if __name__ == '__main__':
#     asyncio.run(run_cbssports_team_names_collector())



