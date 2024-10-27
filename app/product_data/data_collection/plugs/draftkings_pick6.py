from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup
import asyncio

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league, clean_position


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get market name, if exists then execute
    if market := data.get('marketName'):
        # clean the market name
        market = clean_market(market)
        # return the market id from the database and the cleaned market name
        return get_market_id(Market(market, league)), market


def extract_line(data: dict) -> Optional[str]:
    # get dictionary of data, if exists then execute
    if active_market := data.get('activeMarket'):
        # return the over/under numeric line for the prop line
        return active_market.get('targetValue')


def extract_team(data: dict) -> Optional[str]:
    # get the team data dictionary, if exists then execute
    if team_data := data.get('team'):
        # return the player's team name abbreviated, if it doesn't exist then will return None
        return team_data.get('abbreviation')


def extract_position(data: dict) -> Optional[str]:
    # get the position, execute if it exists
    if position := data.get('positionName'):
        # clean the position, conditional is for when secondary positions are also given
        return clean_position(position.split('/')[0] if '/' in position else position)


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the subject name and the competitions dict, if both exist then execute
    if (subject := data.get('displayName')) and (competitions := data.get('pickableCompetitions')):
        # clean the subject name
        subject = clean_subject(subject)
        # get the team that the player is on, get the position that the player plays, return subject id from db and subject name
        return get_subject_id(Subject(subject, league, extract_team(competitions[0]), extract_position(competitions[0]))), subject


class DraftKingsPick6(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # make call to parent class Plug
        super().__init__(info, batch_id, req_mngr)
        # get universal request headers used for many requests
        self.headers = self.req_packager.get_headers()

    async def start(self) -> None:
        # get url to request sports
        url = self.req_packager.get_url()
        # make a request to get the sports
        await self.req_mngr.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response) -> None:
        # only execute if it was a successful response
        if response.status_code == 200:
            # initialize a tasks list to store all requests to be made and then a html parser to extract data
            tasks, soup = [], BeautifulSoup(response.text, 'html.parser')
            # extract and then iterate over all leagues available from the html page
            for league in [sport_div.text for sport_div in soup.find_all('div', {'class': 'dkcss-7q5fzm'}) if
                           not sport_div.text.isnumeric()]:

                # only execute if league exists and the league is valid
                if league and is_league_valid(league):
                    # clean the league name
                    league = clean_league(league)
                    # create an url based and insert a league param for prop lines request
                    url = response.url + f"?sport={league}&_data=routes%2F_index"
                    # add the request for prop lines to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=self.headers))

            # make all requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # gets the json data from the response and then the redundant data from pickableIdToPickableMap field, executes if they both exist
        if (json_data := response.json()) and (data := json_data.get('pickableIdToPickableMap')):
            # for every prop line in the data
            for prop_line in data.values():
                # get pickable data and market_category data, if both exist then execute
                if (pickable := prop_line.get('pickable')) and (market_category := pickable.get('marketCategory')):
                    # get the market id from the db and extract the market from the data dict
                    market_id, market = extract_market(market_category, league)
                    # only execute if market id and market exist
                    if market_id and market:
                        # get the over/under numeric line for the prop line, execute if exists
                        if line := extract_line(prop_line):
                            # for each subject in the pickableEntities if they exist
                            for entity in pickable.get('pickableEntities', []):
                                # get the subject id from the db and extract the subject from data
                                subject_id, subject = extract_subject(entity, league)
                                # execute if both subject id and subject exist
                                if subject_id and subject:
                                    # for each label Over and Under update shared data prop lines
                                    for label in ['Over', 'Under']:
                                        # update shared data
                                        self.add_and_update({
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
                                            'line': line,
                                            'odds': self.info.default_payout.odds
                                        })


if __name__ == "__main__":
    asyncio.run(run(DraftKingsPick6))
