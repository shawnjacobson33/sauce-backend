import asyncio
import pprint
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from app.data_collection.games import utils as gm_utils


# TODO: Same as NHL
def get_current_nba_season():
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


def convert_to_datetime(date_str, time_str):
    # Parse the date string
    date_obj = datetime.strptime(date_str, "%a, %b %d, %Y")

    # Append "AM" or "PM" to the time string based on "a" or "p" suffix
    if time_str[-1].lower() == 'p':
        time_str = time_str[:-1] + "PM"
    elif time_str[-1].lower() == 'a':
        time_str = time_str[:-1] + "AM"

    # Parse the updated time string
    time_obj = datetime.strptime(time_str, "%I:%M%p")

    # Combine date and time
    combined_datetime = date_obj.replace(hour=time_obj.hour, minute=time_obj.minute)

    return combined_datetime


def extract_game_date(tr_elem) -> Optional[str]:
    # extracts the date from the row element if it exists
    if th_elem := tr_elem.find('th'):
        return th_elem.text


def extract_data(tr_elem, attr_name: str) -> Optional[str]:
    # extracts any text from a data cell in the row for a given attribute name
    if td_elem := tr_elem.find('td', {'data-stat': attr_name}):
        return td_elem.text


def extract_game_time(row) -> Optional[str]:
    # get the start time of the game if it exists
    if (game_date := extract_game_date(row)) and (start_time := extract_data(row, 'game_start_time')):
        # convert it to a comparable datetime object and then cast to a string
        return str(convert_to_datetime(game_date, start_time))


def extract_box_score_url(tr_elem) -> Optional[str]:
    # extracts the url pointing to the corresponding box score for this game
    if td_elem := tr_elem.find('td', {'data-stat': 'box_score_text'}):
        if a_elem := td_elem.find('a'):
            return a_elem.get('href')


class NBAScheduleCollector(gm_utils.ScheduleCollector):
    def __init__(self, source_info: gm_utils.Source):
        super().__init__(source_info)

    async def collect(self):
        # get the url for basketball-reference.com's nba schedules
        url = gm_utils.get_url(self.source_info.name, self.source_info.league, 'schedule')
        # get the current month and season (november 2025)
        current_month = datetime.now().strftime("%B").lower()
        current_season = get_current_nba_season()
        # format the url with the dates
        formatted_url = url.format(current_season, current_month)
        # asynchronously request the data and call parse schedule
        await gm_utils.fetch(formatted_url, self._parse_schedule)

    async def _parse_schedule(self, html_content):
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        table = soup.find('table', {'id': 'schedule'})
        # extracts all rows except for the header row from the table
        rows = table.find_all('tr')[1:]
        # for each row
        for row in rows:
            # adds the game and all of its extracted data to the shared data structure
            self.update_games({
                'time_processed': str(datetime.now()),
                "game_time": extract_game_time(row),
                "away_team": extract_data(row, 'visitor_team_name'),
                "home_team": extract_data(row, 'home_team_name'),
                "box_score_url": extract_box_score_url(row),
                "game_notes": extract_data(row, 'game_remarks')
            })


async def main():
    from app.data_collection.games.utils.shared_data import Games
    source = gm_utils.Source('Reference', 'NBA')
    await NBAScheduleCollector(source).collect()
    pprint.pprint(Games.get_games())

if __name__ == '__main__':
    asyncio.run(main())
