import asyncio
from datetime import datetime
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.bookmakers.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league, clean_position


def extract_league(data: dict) -> Optional[str]:
    # get some dictionaries that hold league name, if both exist then execute
    if (match := data.get('match')) and (player_league := match.get('league')):
        # get the league name, if exists then execute
        if league := player_league.get('leagueNameShort'):
            # get the cleaned version of the league
            cleaned_league = clean_league(league)
            # check for validity of the league name
            if is_league_valid(cleaned_league):
                # return the clean and valid league
                return cleaned_league


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position
        return clean_position(position)


def extract_subject_team(data: dict) -> Optional[str]:
    # get the dictionary holding player's team, if exists execute
    if team_data := data.get('team'):
        # return the player's abbreviated team name
        return team_data.get('teamAbbreviation')


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get a dictionary holding player attributes, if exists keep executing
    if (player_data := data.get('player')) and (subject := player_data.get('fullName')):
        # clean the subject's name
        cleaned_subject = clean_subject(subject)
        # get some player attributes
        subject_team, position = extract_subject_team(player_data), extract_position(player_data)
        # get the subject id from the db
        if subject_id := get_subject_id(Subject(cleaned_subject, league, team=subject_team, position=position)):
            # cast the subject id to a string
            subject_id = str(subject_id)

        # return the subject id from db and the cleaned subject name
        return subject_id, cleaned_subject


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the dictionary that holds market data and get market name, if both exist then execute
    if market := data.get('challengeName'):
        # get the cleaned market name
        cleaned_market = clean_market(market, 'parlay_play', league=league)
        # get the market id from the db
        if market_id := get_market_id(Market(cleaned_market, league)):
            # cast the market id to a string
            market_id = str(market_id)

        # return the market id from the db and the cleaned market name
        return market_id, cleaned_market


def extract_odds_and_label(data: dict) -> Optional[tuple[str, str]]:
    # get odds and labels
    odds_and_labels = zip([data.get('decimalPriceOver'), data.get('decimalPriceUnder')], ['Over', 'Under'])
    for odds, label in odds_and_labels:
        if odds:
            yield odds, label
    # # yield for each over and under label and over odds or under odds corresponding, if the odds for a label exist
    # yield from ((odds, label) for odds, label in odds_and_labels if odds)


class ParlayPlay(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def start(self) -> None:
        # get the url that is required to make requests for prop lines
        url = self.req_packager.get_url()
        # get the headers required to make requests for prop lines
        headers = self.req_packager.get_headers()
        # make the request for prop lines
        await self.req_mngr.get(url, self._parse_lines, headers=headers)

    async def _parse_lines(self, response) -> None:
        # get the response data, if exists then keep executing
        if json_data := response.json():
            # for each player in the response data's players if they exist
            for player_data in json_data.get('players', []):
                # extract the league, if it exists then keep executing
                if league := extract_league(player_data):
                    # get the subject id from db and extract the subject name from a dictionary
                    subject_id, subject = extract_subject(player_data, league)
                    # if both subject id and subject exist then keep executing
                    if subject_id and subject:
                        # for each stat dictionary in the player data dictionary if they exist
                        for stat_data in player_data.get('stats', []):
                            # get the market id from the db and extract the market
                            market_id, market = extract_market(stat_data, league)
                            # if both exist then keep executing
                            if market_id and market:
                                # get a dictionary of data around market, lines, and odds
                                if alt_lines_data := stat_data.get('altLines'):
                                    # for each dictionary holding line, odds data if values exist
                                    for line_data in alt_lines_data.get('values', []):
                                        # if the numeric over/under line exists get it and execute
                                        if line := line_data.get('selectionPoints'):
                                            # for each over and under label
                                            for odds, label in extract_odds_and_label(line_data):
                                                # update shared data
                                                self.add_and_update({
                                                    'batch_id': self.batch_id,
                                                    'time_processed': str(datetime.now()),
                                                    'league': league,
                                                    'market_category': 'player_props',
                                                    'market_id': market_id,
                                                    'market': market,
                                                    'subject_id': subject_id,
                                                    'subject': subject,
                                                    'bookmaker': self.info.name,
                                                    'label': label,
                                                    'line': line,
                                                    'odds': odds,
                                                    'is_boosted': stat_data.get('isBoostedPayout')
                                                })


if __name__ == "__main__":
    asyncio.run(run(ParlayPlay))
    Plug.save_to_file()
