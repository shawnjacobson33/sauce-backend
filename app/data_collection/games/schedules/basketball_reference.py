import uuid
import asyncio
import pprint
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from app.data_collection import utils as dc_utils
from app.data_collection.games import utils as gm_utils


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


def extract_team(source_name: str, league: str, row, attr_name: str) -> Optional[dict[str, str]]:
    # get the team name from the html
     if team_name := extract_data(row, attr_name):
         # get the team id and team name from the database
         if team_data := dc_utils.get_team_id(source_name, league, team_name):
             # return the team id and team name
             return team_data


def extract_game_time(row, valid_dates: list[datetime]) -> Optional[str]:
    # get the start time of the game if it exists
    if (game_date := extract_game_date(row)) and (start_time := extract_data(row, 'game_start_time')):
        # convert it to a comparable datetime object
        game_datetime = convert_to_datetime(game_date, start_time)
        # make sure that this game is in the desired date range
        if game_datetime.strftime("%Y-%m-%d") in valid_dates:
            # cast to a string and return
            return str(game_datetime)


def extract_box_score_url(tr_elem) -> Optional[str]:
    # extracts the url pointing to the corresponding box score for this game
    if th_elem := tr_elem.find('th', {'data-stat': 'date_game'}):
        # get the link holding box score url
        if box_score_identifier := th_elem.get('csk'):
            # return both if both exist
            return f'/boxscores/{box_score_identifier}.html'


class NBAScheduleCollector(gm_utils.ScheduleCollector):
    def __init__(self, source_info: gm_utils.Source):
        super().__init__(source_info)

    async def retrieve(self, n_days: int = 1):
        # generate a range of dates predicated upon n_days param
        date_list = gm_utils.get_date_range(n_days)
        # Get the month and season for each date (if needed)
        target_months = [datetime.strptime(date, "%Y-%m-%d").strftime("%B").lower() for date in date_list]
        # Get the URL for the NBA schedule
        url = gm_utils.get_url(self.source.name, 'schedule')
        # Use each date’s month to format URLs and fetch data
        for month in target_months:
            # format the url with the current season (2025) and month
            formatted_url = url.format(gm_utils.CURR_SEASON_1, month)
            # Asynchronously request the data and call parse schedule for each formatted URL
            await gm_utils.fetch(formatted_url, self._parse_schedule, date_list)

    async def _parse_schedule(self, html_content, dates: list[datetime]):
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        table = soup.find('table', {'id': 'schedule'})
        # extracts all rows except for the header row from the table
        rows = table.find_all('tr')[1:]
        # for each row
        for row in rows:
            # get the time and date of the game and check if it's in the right range of dates desired
            if game_time := extract_game_time(row, dates):
                # get the away team name and id if it exists
                if away_team := extract_team(self.source.name, self.source.league, row,
                                             'visitor_team_name'):
                    # get the home team name and id if it exists
                    if home_team := extract_team(self.source.name, self.source.league, row,
                                                 'home_team_name'):
                            # adds the game and all of its extracted data to the shared data structure
                            self.update_games({
                                'time_processed': str(datetime.now()),
                                'source': self.source.name,
                                "league": self.source.league,
                                "game_time": game_time,
                                "away_team": away_team,
                                "home_team": home_team,
                                "box_score_url": extract_box_score_url(row),
                                "game_notes": extract_data(row, 'game_remarks')
                            })


async def main():
    from app.data_collection.utils.shared_data import Games
    source = gm_utils.Source('basketball-reference', 'NBA')
    await NBAScheduleCollector(source).collect(n_days=2)
    pprint.pprint(Games.get_games())

if __name__ == '__main__':
    asyncio.run(main())
