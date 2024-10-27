from datetime import datetime, timedelta
import asyncio
from typing import Optional

from app.product_data.data_collection.utils.requesting import RequestManager
from app.product_data.data_collection.utils.objects import Subject, Market, Plug, Bookmaker
from app.product_data.data_collection.utils.standardizing import get_subject_id, get_market_id
from app.product_data.data_collection.plugs.helpers import run, is_league_valid, clean_market, clean_subject, \
    clean_league


# Different market names for the same market for NFL and NCAAF -- need to normalize
MARKET_MAP = {
    'Total Rushing + Receiving Yards': 'Total Rush + Rec Yards',
    'Total Passing + Rushing Yards': 'Total Pass + Rush Yards',
    'Total Passing + Rushing + Receiving TDs': 'Total Pass + Rush + Rec TDs',
}
BOOKMAKER_MAP = {
    'Underdog': 'Underdog Fantasy',
    'Pick6': 'DraftKingsPick6',
}


def extract_league(data: dict) -> Optional[str]:
    # get league and only execute if exists
    if league := data.get('leagueCode'):
        # clean the league name
        cleaned_league = clean_league(league)
        # league must be valid
        if is_league_valid(league):
            # return the valid and clean league name
            return cleaned_league


def get_dates() -> tuple[str, str]:
    # desired date_format
    date_format = '%Y-%m-%dT%H:%M:%S.%f'
    # Calculate and return current datetime and future date
    return datetime.now().strftime(date_format)[:-3] + 'Z', (datetime.now() + timedelta(days=8)).strftime(date_format)[:-3] + 'Z'


def extract_market(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the market name, if it exists keep executing
    if market := data.get('offerName'):
        # Check if the market exists in the map to format differently
        market = MARKET_MAP.get(market, market)
        # clean the market name
        market = clean_market(market, league)
        # return market id from the database and the clean market name
        return get_market_id(Market(market, league)), market


def extract_subject(data: dict, league: str) -> Optional[tuple[str, str]]:
    # get the subject name from data, only keep going if it exists
    if subject := data.get('label'):
        # clean the subject name
        cleaned_subject = clean_subject(subject)
        # return the subject id from the db and the cleaned subject name
        return get_subject_id(Subject(cleaned_subject, league), user='OddsShopper'), cleaned_subject


def extract_bookmaker(data: dict) -> Optional[str]:
    # get the bookmaker from the dictionary
    if bookmaker := data.get('sportsbookCode'):
        # return the bookmaker after checking if it needs formatting...to standardize across other plugs
        return BOOKMAKER_MAP.get(bookmaker, bookmaker)


def extract_true_win_probability(data: dict) -> Optional[float]:
    # gets the true win probability, if exists then execute
    if tw_prob := data.get('trueWinProbability'):
        # return a rounded true win probability by 3 decimal points
        return round(tw_prob, 3)


def extract_expected_value(data: dict) -> Optional[float]:
    # get the expected value, if it exists then execute
    if ev := data.get('ev'):
        # return the rounded expected value by 3 decimal points
        return round(ev, 3)


def extract_odds_and_other_stats(data: dict) -> Optional[tuple[float, float, float, float]]:
    # get the odds from dictionary, only execute if it exists
    if odds := data.get('odds'):
        # return the odds rounded by 3 decimal points, implied probability, true win probability, and expected value (calculated by OddsShopper)
        return round(odds, 3), round(1 / odds, 3), extract_true_win_probability(data), extract_expected_value(data)


class OddsShopper(Plug):
    def __init__(self, info: Bookmaker, batch_id: str, req_mngr: RequestManager):
        # call parent class Plug
        super().__init__(info, batch_id, req_mngr)

    async def start(self) -> None:
        # get the url to request matchups data
        url = self.req_packager.get_url(name='matchups')
        # get the headers to request matchups data
        headers = self.req_packager.get_headers()
        # get the cookies to request matchups data
        cookies = self.req_packager.get_cookies()
        # make request for matchups data
        await self.req_mngr.get(url, self._parse_matchups, headers=headers, cookies=cookies)

    async def _parse_matchups(self, response) -> None:
        # get json data from response and check its existence
        if json_data := response.json():
            # initialize tasks to hold all the requests to be made
            tasks = []
            # for each offer category dictionary in the response data's offerCategories if they exist
            for offer_category in json_data.get('offerCategories', []):
                # get the market category, only want data...FOR NOW... that is player props
                if offer_category.get('name') == 'PlayerProps':
                    # for each league dictionary of data in offer_category's offers if they exist
                    for league_data in offer_category.get('offers', []):
                        # extract the league from response data and offer id, only if both exist should it execute
                        if (league := extract_league(league_data)) and (offer_id := league_data.get('id')):
                            # get dates which are required for params of url for prop lines
                            start_date, end_date = get_dates()
                            # get the url required to request prop lines and format it with the offer id
                            url = self.req_packager.get_url().format(offer_id)
                            # get the headers required to request prop lines
                            headers = self.req_packager.get_headers()
                            # get the params required to request prop lines, using start_date, and end date
                            params = self.req_packager.get_params(var_1=start_date, var_2=end_date)
                            # add the request to tasks
                            tasks.append(self.req_mngr.get(url, self._parse_lines, league, headers=headers, params=params))

            # add all requests to the event loop and make the asynchronously
            await asyncio.gather(*tasks)

    async def _parse_lines(self, response, league: str) -> None:
        # get json data from response and check its existence
        if json_data := response.json():
            # for each event/game in the response data
            for event in json_data:
                # get the market id from db and extract market from the data
                market_id, market = extract_market(event, league)
                # only keep executing if market id and market exist
                if market_id and market:
                    # iterate through each side or bookmaker for the prop line
                    for side in event.get('sides', []):
                        # get the label from the side dictionary, only keep going if it exists
                        if label := side.get('label'):
                            # iterate through each outcome for each side
                            for outcome in side.get('outcomes', []):
                                # get the subject id from the db and extract the subject from outcome
                                subject_id, subject = extract_subject(outcome, league)
                                # only if both exist should execution keep going
                                if subject_id and subject:
                                    # get the bookmaker from outcome dictionary, if exists keep executing
                                    if bookmaker := extract_bookmaker(outcome):
                                        # extract odds, implied probability, true win probability, and expected value if they exist
                                        odds, impl_prob, tw_prob, ev = extract_odds_and_other_stats(outcome)
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
                                            'bookmaker': bookmaker,
                                            'label': label,
                                            'line': outcome.get('line', '0.5'),
                                            'odds': odds,
                                            'implied_prob': impl_prob,
                                            'other_stats': {
                                                'true_win_prob': tw_prob,
                                                'ev': ev
                                            }
                                        }, bookmaker=bookmaker)


if __name__ == "__main__":
    asyncio.run(run(OddsShopper))
