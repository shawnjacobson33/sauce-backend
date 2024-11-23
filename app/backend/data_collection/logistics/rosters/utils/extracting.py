from typing import Optional

from bs4 import BeautifulSoup

from app.backend.data_collection.logistics import utils as lg_utils
from app.backend.data_collection.utils import Source


CONFERENCE_MAP = {
    'NCAA': {
        'ACC', 'Big East', 'Big Ten', 'Big 12', 'Ivy', 'Mid American', 'Mountain West', 'SEC', 'West Coast'
    }
}


async def extract_teams(source: Source) -> Optional[list[tuple[str, str]]]:
    # get the url to retrieve teams
    url = lg_utils.get_url(source, 'teams')
    # get the conferences for the particular league if not NCAA then just get every conference
    conferences = CONFERENCE_MAP.get(source.league, 'all')
    # make the request
    return await lg_utils.fetch(url, parse_teams, conferences)


def parse_teams(html_content, conferences, league: str) -> Optional[list[tuple[str, str]]]:
    # initialize a parser
    soup = BeautifulSoup(html_content, 'html.parser')
    # get all tables that exist
    if tables := soup.find_all('table'):
        # get the conference names that exist
        if conference_names := soup.find_all('span', {'class': 'TeamLogoNameLockup-name'}):
            # for zipping we want to make sure that there are the same number of tables as conference headers
            if len(tables) == len(conference_names):
                # create a list to store the teams
                teams = list()
                # loop through every table and conference name
                for table, conference_name in zip(tables, conference_names):
                    # only want particular conference names for college sports
                    if (conferences == 'all') or (conference_name.text.strip() in conferences):
                        # get all rows or teams in the table
                        if (rows := table.find_all('tr')) and (len(rows) > 1):
                            # for each row excluding the header
                            for row in rows[1:]:
                                # for NFL there are extra header rows within a table
                                if row.get('class') != 'TableBase-headTr':
                                    # get the span element that holds the team name
                                    if team_name_span := row.find('span', {'class': 'TeamName'}):
                                        # get the link element and url
                                        if (a_elem := team_name_span.find('a')) and (href := a_elem.get('href')):
                                            # split up the url into components
                                            href_comps = href.split("/")
                                            # if it meets a certain length threshold for indexing
                                            if len(href_comps) > 3:
                                                # extract the abbreviated and full team name
                                                abbr_name, full_name = href_comps[3], a_elem.text
                                                # store a tuple of both names in the list
                                                teams.append({
                                                    'abbr_name': abbr_name,
                                                    'full_name': full_name,
                                                    'league': "NCAA"
                                                })

                # return the list of team names
                return teams


# import requests
# from app.backend import database as db
#
# source = Source('cbssports-ncaaf', 'NCAAF')
# # get the url to retrieve teams
# url = lg_utils.get_url(source, 'teams')
# # get the conferences for the particular league if not NCAA then just get every conference
# conferences = CONFERENCE_MAP.get(source.league, 'all')
# # make the request
# response = requests.get(url)
#
# teams = parse_teams(response.text, conferences, source.league)
#
# db.MongoDB.fetch_collection(TEAMS_COLLECTION_NAME).insert_many(teams)

