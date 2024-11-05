from datetime import datetime
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils


def extract_league(data: dict) -> Optional[str]:
    # get some dictionaries that hold league name, if both exist then execute
    if (match := data.get('match')) and (player_league := match.get('league')):
        # get the league name, if exists then execute
        if league := player_league.get('leagueNameShort'):
            # get the cleaned version of the league
            cleaned_league = utils.clean_league(league)
            # check for validity of the league name
            if utils.is_league_valid(cleaned_league):
                # return the clean and valid league
                return cleaned_league


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists then execute
    if position := data.get('position'):
        # return the cleaned position
        return utils.clean_position(position)


def extract_subject_team(data: dict) -> Optional[str]:
    # get the dictionary holding player's team, if exists execute
    if team_data := data.get('team'):
        # return the player's abbreviated team name
        return team_data.get('teamAbbreviation')


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get a dictionary holding player attributes, if exists keep executing
    if (player_data := data.get('player')) and (subject_name := player_data.get('fullName')):
        # get some player attributes
        subject_team, position = extract_subject_team(player_data), extract_position(player_data)
        # create a subject object
        subject_obj = utils.Subject(subject_name, league, team=subject_team, position=position)
        # gets the subject id or log message
        subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
        # return both subject id search result and cleaned subject
        return subject_id, subject_name

    return None, None


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the dictionary that holds market data and get market name, if both exist then execute
    if market_name := data.get('challengeName'):
        # create a market object
        market_obj = utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name

    return None, None


def extract_odds_and_label(data: dict) -> Optional[tuple[str, str]]:
    # get odds and labels
    odds_and_labels = zip([data.get('decimalPriceOver'), data.get('decimalPriceUnder')], ['Over', 'Under'])
    for odds, label in odds_and_labels:
        if odds:
            yield odds, label
    # # yield for each over and under label and over odds or under odds corresponding, if the odds for a label exist
    # yield from ((odds, label) for odds, label in odds_and_labels if odds)

# TODO: TWEAK SO YOU GET SPORTS FROM https://parlayplay.io/api/v1/sports/ AND THEN USE THEM AS PARAMS IN PROP LINES URL
class ParlayPlay(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)

    async def collect(self) -> None:
        # get the url that is required to make requests for prop lines
        url = utils.get_url(self.bookmaker_info.name)
        # get the headers required to make requests for prop lines
        headers = utils.get_headers(self.bookmaker_info.name)
        # make the request for prop lines
        await self.req_mngr.get(url, self._parse_lines, headers=headers)

    # TODO: NOT GETTING ALL LINES ON THE APP?
    async def _parse_lines(self, response) -> None:
        # get the response data, if exists then keep executing
        if json_data := response.json():
            # for each player in the response data's players if they exist
            for player_data in json_data.get('players', []):
                # extract the league, if it exists then keep executing
                if league := extract_league(player_data):
                    # to track the leagues being collected
                    self.metrics.add_league(league)
                    # get the subject id from db and extract the subject name from a dictionary
                    subject_id, subject_name = extract_subject(self.bookmaker_info.name, player_data, league)
                    # if both subject id and subject exist then keep executing
                    if subject_id and subject_name:
                        # to track the subjects being collected
                        self.metrics.add_subject((league, subject_name))
                        # for each stat dictionary in the player data dictionary if they exist
                        for stat_data in player_data.get('stats', []):
                            # get the market id from the db and extract the market
                            market_id, market_name = extract_market(self.bookmaker_info.name, stat_data, league)
                            # if both exist then keep executing
                            if market_id and market_name:
                                # to track the markets being collected
                                self.metrics.add_market((league, market_name))
                                # get a dictionary of data around market, lines, and odds
                                if alt_lines_data := stat_data.get('altLines'):
                                    # for each dictionary holding line, odds data if values exist
                                    for line_data in alt_lines_data.get('values', []):
                                        # if the numeric over/under line exists get it and execute
                                        if line := line_data.get('selectionPoints'):
                                            # for each over and under label
                                            for odds, label in extract_odds_and_label(line_data):
                                                # update shared data
                                                self.update_betting_lines({
                                                    'batch_id': self.batch_id,
                                                    'time_processed': str(datetime.now()),
                                                    'league': league,
                                                    'market_category': 'player_props',
                                                    'market_id': str(market_id),
                                                    'market': market_name,
                                                    'subject_id': str(subject_id),
                                                    'subject': subject_name,
                                                    'bookmaker': self.bookmaker_info.name,
                                                    'label': label,
                                                    'line': line,
                                                    'odds': odds,
                                                    'is_boosted': stat_data.get('isBoostedPayout')
                                                })


if __name__ == "__main__":
    utils.run(ParlayPlay)
