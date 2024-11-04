from datetime import datetime
import asyncio
from typing import Optional

from app.product_data.data_collection.plugs.bookmakers import utils


def extract_league(data: dict) -> Optional[str]:
    # get the league, if exists then execute
    if league := data.get('sport'):
        # return the cleaned league name
        return utils.clean_league(league)


def extract_market(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get some market dictionary and then the market name from it, if both exist keep going
    if (market_type := data.get('marketType')) and (market_name := market_type.get('name')):
        # create a market object
        market_obj = utils.Market(market_name, league=league)
        # gets the market id or log message
        market_id, market_name = utils.get_market_id(bookmaker_name, market_obj)
        # return both market id search result and cleaned market
        return market_id, market_name


def extract_position(data: dict) -> Optional[str]:
    # get the player's position, if exists keep executing
    if position := data.get('position'):
        # return the cleaned position string
        return utils.clean_position(position)


def extract_subject_team(data: dict) -> Optional[str]:
    # get the subject team, if exists then keep executing
    if subject_team := data.get('teamAlias'):
        # return the uppercase version of subject team
        return subject_team.upper()


def extract_subject(bookmaker_name: str, data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the player data, if it exists then keep executing
    if player_data := data.get('player'):
        # get the first nam and last name of the player, if both exist then keep executing
        if (first_name := player_data.get('firstName')) and (last_name := player_data.get('lastName')):
            # get some player attributes
            subject_team, position = extract_subject_team(player_data), extract_position(player_data)
            # create subject object
            subject_obj = utils.Subject(' '.join([first_name, last_name]), league, team=subject_team, position=position)
            # gets the subject id or log message
            subject_id, subject_name = utils.get_subject_id(bookmaker_name, subject_obj)
            # return both subject id search result and cleaned subject
            return subject_id, subject_name


def extract_line(data: dict) -> Optional[str]:
    # get some dictionary that holds the numeric line, if exists then execute
    if balanced_line := data.get('line'):
        # return the numeric over/under line
        return balanced_line.get('balancedLine')


def extract_game_info(data: dict) -> Optional[str]:
    # get some game data dictionary, if exists then execute
    if game_data := data.get('game'):
        # get the away team and home team, if both exist then keep executing
        if (away_team := game_data.get('awayTeam')) and (home_team := game_data.get('homeTeam')):
            # get better formatted versions of teams, if exist keep going
            if (away_team_alias := away_team.get('alias')) and (home_team_alias := home_team.get('alias')):
                # uppercase both team names
                away_team_alias, home_team_alias = away_team_alias.upper(), home_team_alias.upper()
                # return the game info
                return ' @ '.join([away_team_alias, home_team_alias])


def extract_labels(label: str) -> Optional[list[str]]:
    # split up the different components of the label string by underscore
    label_comps =  label.split('_')
    # make sure there are enough elements to index
    if len(label_comps) > 1:
        # get the labels based on other keyword in the label components (MORE, OR, LESS....MORE, ONLY)
        connecting_word = label_comps[1]
        # if the word is 'or'
        if connecting_word == 'OR':
            # return both labels in a list
            return ['Over', 'Under']

        # if the word is 'only'
        elif connecting_word == 'ONLY':
            # get the label as 'Over' keyword is 'MORE' else 'Under'
            formatted_label = 'Over' if label_comps[0] == 'MORE' else 'Under'
            # return the singular label in a list
            return [formatted_label]


def get_label(data: dict) -> Optional[str]:
    # get some dictionary holding data about whether the prop line is over/under type of pick, if exists then execute
    if pick_options := data.get('pickOptions'):
        # iterate over each general label for a prop line
        for label in extract_labels(pick_options):
            # yield the label
            yield label


class OwnersBox(utils.BookmakerPlug):
    def __init__(self, bookmaker_info: utils.Bookmaker, batch_id: str):
        # call parent class Plug
        super().__init__(bookmaker_info, batch_id)
        # get the universal headers required to make requests
        self.headers = utils.get_headers(self.bookmaker_info.name)
        # get the universal cookies required to make requests
        self.cookies = utils.get_cookies(self.bookmaker_info.name)

    async def collect(self) -> None:
        # get the url required to make request for leagues data
        url = utils.get_url(self.bookmaker_info.name, name='leagues')
        # make the request for the leagues data
        await self.req_mngr.get(url, self._parse_leagues, headers=self.headers, cookies=self.cookies)

    async def _parse_leagues(self, response) -> None:
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # initialize structure to hold all requests to make
            tasks = []
            # get the url required to make requests for markets data
            url = utils.get_url(self.bookmaker_info.name, name='markets')
            # for each league in the response data dictionary
            for league in json_data:
                # only execute if the league is valid...also clean the league before checking
                if utils.is_league_valid(utils.clean_league(league)):
                    # if valid get the params to make the request required to get markets data for this league
                    params = utils.get_params(self.bookmaker_info.name, name='markets', var_1=league)
                    # add the request to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_markets, league, headers=self.headers, cookies=self.cookies, params=params))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_markets(self, response, league: str) -> None:
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # initialize structure to hold all requests to make
            tasks = []
            # get the url required to make the request for prop lines
            url = utils.get_url(self.bookmaker_info.name)
            # for each market in the response data
            for market in json_data:
                # get market id (not from db), if exists add request
                if market_id := market.get('id'):
                    # get the params required to make request for prop lines
                    params = utils.get_params(self.bookmaker_info.name, var_1=league, var_2=market_id)
                    # add the request to tasks
                    tasks.append(self.req_mngr.get(url, self._parse_lines, headers=self.headers, cookies=self.cookies, params=params))

            # start making requests asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response) -> None:
        # gets the json data from the response, if exists then execute
        if json_data := response.json():
            # for each prop line in response data's markets if they exist
            for prop_line_data in json_data.get('markets', []):
                # get the league name, if exists then keep executing
                if league := extract_league(prop_line_data):
                    # to track the leagues being collected
                    self.metrics.add_league(league)
                    # get the market id from db and extract the market name
                    market_id, market_name = extract_market(self.bookmaker_info.name, prop_line_data, league)
                    # # if both exist continue executing
                    if market_id and market_name:
                        # to track the markets being collected
                        self.metrics.add_market((league, market_name))
                        # get the subject id from db and extract the subject name from the dictionary
                        subject_id, subject_name = extract_subject(self.bookmaker_info.name, prop_line_data, league)
                        # if both exist then continue executing
                        if subject_id and subject_name:
                            # to track the subjects being collected
                            self.metrics.add_subject((league, subject_name))
                            # get the numeric over/under line, execute if exists
                            if line := extract_line(prop_line_data):
                                # get game info
                                game_info = extract_game_info(prop_line_data)
                                # for each label that the prop line has
                                for label in get_label(prop_line_data):
                                    # update the shared data
                                    self.update_betting_lines({
                                        'batch_id': self.batch_id,
                                        'time_processed': str(datetime.now()),
                                        'league': league,
                                        'game_info': game_info,
                                        'market_category': 'player_props',
                                        'market_id': str(market_id),
                                        'market': market_name,
                                        'subject_id': str(subject_id),
                                        'subject': subject_name,
                                        'bookmaker': self.bookmaker_info.name,
                                        'label': label,
                                        'line': line,
                                        'odds': self.bookmaker_info.default_payout.odds
                                    })


if __name__ == "__main__":
    utils.run(OwnersBox)
