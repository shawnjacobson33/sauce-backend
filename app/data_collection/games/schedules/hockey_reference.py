import asyncio
import pprint
from datetime import datetime
from typing import Optional, Union, Any

from bs4 import BeautifulSoup

from app.data_collection.games import utils as gm_utils


# TODO: Same as NBA
def get_current_nhl_season():
    now = datetime.now()
    year = now.year
    month = now.month

    # Determine the season based on the month
    if month >= 10:  # October to December
        season = f"{year + 1}"
    elif month <= 6:  # January to June
        season = f"{year}"
    else:  # July to September (off-season)
        season = f"{year}"

    return season


# TODO: Same as NFL
def convert_to_datetime(date_str, time_str):
    # Parse the date string
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")

    # Parse the time string with AM/PM format
    time_obj = datetime.strptime(time_str, "%I:%M %p")

    # Combine the parsed date with the parsed time
    result = date_obj.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)

    return result


def extract_data(tr_elem, attr_name: str) -> Optional[str]:
    # extracts any text from a data cell in the row for a given attribute name
    if td_elem := tr_elem.find('td', {'data-stat': attr_name}):
        return td_elem.text


def extract_game_date_and_box_score_url(tr_elem) -> Union[tuple[Any, Any], tuple[None, None]]:
    # extracts the date from the row element if it exists
    if th_elem := tr_elem.find('th', {'data-stat': 'date_game'}):
        # get the link holding box score url
        if a_elem := th_elem.find('a'):
            # return both if both exist
            return a_elem.text, a_elem.get('href')

        # if the game hasn't happened yet
        return th_elem.text, None

    return None, None


def extract_game_time_and_box_score_url(row) -> Optional[Union[tuple[None, None], tuple[str, str]]]:
    # get the game date and box score url
    game_date, box_score_url = extract_game_date_and_box_score_url(row)
    # get the game date and start time of the game if it exists
    if game_date and (start_time := extract_data(row, 'time_game')):
        # convert it to a comparable datetime object and then cast to a string, also return box score url
        return str(convert_to_datetime(game_date, start_time)), box_score_url

    return None, None


class NHLScheduleCollector(gm_utils.ScheduleCollector):
    def __init__(self, source_info: gm_utils.Source):
        super().__init__(source_info)

    async def collect(self) -> None:
        # get the url for hockey-reference.com's nhl schedule
        url = gm_utils.get_url(self.source_info.name, self.source_info.league, 'schedule')
        # get the current season how it needs to be formatted
        current_season = get_current_nhl_season()
        # format the url with the dates
        formatted_url = url.format(current_season)
        # asynchronously request the data and call parse schedule
        await gm_utils.fetch(formatted_url, self._parse_schedule)

    async def _parse_schedule(self, html_content) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        table = soup.find('table', {'id': 'games'})
        # extracts all rows except for the header row from the table
        rows = table.find_all('tr')[1:]
        # for each row
        for row in rows:
            # get both game time formatted and box score url
            game_time, box_score_url = extract_game_time_and_box_score_url(row)
            # adds the game and all of its extracted data to the shared data structure
            self.update_games({
                'time_processed': str(datetime.now()),
                "game_time": game_time,
                "away_team": extract_data(row, 'visitor_team_name'),
                "home_team": extract_data(row, 'home_team_name'),
                "box_score_url": box_score_url,
                "game_notes": extract_data(row, 'game_remarks')  # TODO: Same as NBA
            })


async def main():
    from app.data_collection.games.utils.shared_data import Games
    source = gm_utils.Source('Reference', 'NHL')
    await NHLScheduleCollector(source).collect()
    pprint.pprint(Games.get_games())

if __name__ == '__main__':
    asyncio.run(main())