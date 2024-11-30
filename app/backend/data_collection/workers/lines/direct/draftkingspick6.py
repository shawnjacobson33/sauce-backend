from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup
import asyncio

from app.backend.data_collection.workers import utils as dc_utils
from app.backend.data_collection.workers.lines import utils as ln_utils


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[dict[str, str]]:
    # get market name, if exists then execute
    if market_name := data.get('marketName'):
        # gets the market id or log message
        market = dc_utils.get_market(bookmaker_name, league, market_name)
        # return both market id search result and cleaned market
        return market


def extract_line(data: dict) -> Optional[str]:
    # get dictionary of data, if exists then execute
    if active_market := data.get('activeMarket'):
        # return the over/under numeric line for the prop line
        return active_market.get('targetValue')


def extract_team(bookmaker_name: str, league: str, data: list[dict]) -> Optional[dict[str, str]]:
    # get the team data dictionary, if exists then execute
    if (team_data := data[0].get('team')) and (abbr_team_name := team_data.get('abbreviation')):
        # return the team id and team name
        return dc_utils.get_team(bookmaker_name, league, abbr_team_name)


def extract_position(data: list[dict]) -> Optional[str]:
    # get the position, execute if it exists
    if position := data[0].get('positionName'):
        # clean the position, conditional is for when secondary positions are also given
        return dc_utils.clean_position(position.split('/')[0] if '/' in position else position)


def extract_subject(bookmaker_name: str, data: dict, league: str, team: dict) -> Optional[dict[str, str]]:
    # get the subject name and the competitions dict, if both exist then execute
    if subject_name := data.get('displayName'):
        # gets the subject id or log message
        return dc_utils.get_subject(bookmaker_name, league, subject_name, team=team)


class DraftKingsPick6(ln_utils.LinesRetriever):
    def __init__(self, bookmaker: ln_utils.LinesSource):
        # make call to parent class Plug
        super().__init__(bookmaker)
        # get universal request headers used for many requests
        self.headers = ln_utils.get_headers(self.name)

    async def retrieve(self) -> None:
        # get url to request sports
        url = ln_utils.get_url(self.name)
        # make a request to get the sports
        await self.req_mngr.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response) -> None:
        # only execute if it was a successful response
        if html_content := response.text:
            # initialize a tasks list to store all requests to be made and then a html parser to extract data
            tasks, soup = [], BeautifulSoup(html_content, 'html.parser')
            # extract and then iterate over all leagues available from the html page
            for league in [sport_div.text for sport_div in soup.find_all('div', {'class': 'dkcss-7q5fzm'}) if
                           not sport_div.text.isnumeric()]:

                # only execute if league exists and the league is valid
                if league and ln_utils.is_league_valid(league):
                    # clean the league name
                    league = dc_utils.clean_league(league)
                    # create an url based and insert a league param for prop lines request
                    url = response.url + f"?sport={league}&_data=routes%2F_index"
                    # add the request for prop lines to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=self.headers))

            # make all requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # gets the json data from the response and then the redundant data from pickableIdToPickableMap field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('pickableIdToPickableMap')):
            # get the sport for this league
            sport = dc_utils.LEAGUE_SPORT_MAP[league]
            # to track the leagues being collected
            dc_utils.RelevantData.update_relevant_leagues(league, self.name)
            # for every prop line in the data
            for prop_line_data in data.values():
                # get pickable data and market_category data, if both exist then execute
                if (pick_data := prop_line_data.get('pickable')) and (m_category_data := pick_data.get('marketCategory')):
                    # get the market id from the db and extract the market from the data dict
                    if market := extract_market(self.name, m_category_data, league):
                        # get the over/under numeric line for the prop line, execute if exists
                        if line := extract_line(prop_line_data):
                            # for each subject in the pickableEntities if they exist
                            for entity in pick_data.get('pickableEntities', []):
                                # get a dictionary that holds player attributes data
                                if competitions := entity.get('pickableCompetitions'):
                                    # get player attributes
                                    if team := extract_team(self.name, league, competitions):
                                        # use team data to get some game data
                                        if game := dc_utils.get_game(league, team['abbr_name']):
                                            # get the subject id from the db and extract the subject from data
                                            if subject := extract_subject(self.name, entity, league, team):
                                                # for each label Over and Under update shared data prop lines
                                                for label in ['Over', 'Under']:
                                                    # update shared data
                                                    dc_utils.BettingLines.update({
                                                        's_tstamp': str(datetime.now()),
                                                        'bookmaker': self.name,
                                                        'sport': sport,
                                                        'league': league,
                                                        'game_time': game['game_time'],
                                                        'game': game['info'],
                                                        'market_id': market['id'],
                                                        'market': market['name'],
                                                        'subject_id': subject['id'],
                                                        'subject': subject['name'],
                                                        'label': label,
                                                        'line': line,
                                                        'dflt_odds': self.dflt_odds,
                                                        'dflt_im_prb': self.dflt_im_prb
                                                    })
