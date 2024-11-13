from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.games import utils as gm_utils


def convert_to_datetime(date: datetime, time_str: str) -> datetime:
    # Append "M" to the time string based on "a" or "p" suffix
    if time_str[-1].lower() in ['a', 'p']:
        time_str = time_str.strip() + "m"  # Add 'm' to make it "am" or "pm"

    # Parse the time string
    time_obj = datetime.strptime(time_str, "%I:%M %p")
    # Combine date and time
    combined_datetime = date.replace(hour=time_obj.hour, minute=time_obj.minute)
    # return the combined date
    return combined_datetime


def extract_team(span_elem, source_name: str, league: str) -> Optional[dict[str, str]]:
    # get the link element
    if a_elem := span_elem.find('a'):
        # extract link components
        link_components = a_elem.get('href').split('/')
        # if the link has expected number of components
        if len(link_components) > 3:
            # get the abbreviated team name
            abbr_team_name = ('abbr_name', link_components[3])
            # get the team id and team name from the database
            if team := dc_utils.get_team_id(source_name, league, abbr_team_name):
                # return the team id and team name
                return team


def extract_game_time_and_box_score_url(row, date: datetime) -> Optional[tuple[datetime, str]]:
    # get the start time of the game if it exists
    if game_time_div := row.find('div', {'class': 'CellGame'}):
       # get the link element from the div elem
       if a_elem := game_time_div.find('a'):
           # get the url pointing to box scores
           box_score_url = a_elem.get('href')
           # get the game time converted to a datetime object
           game_time = convert_to_datetime(date, a_elem.text)
           # return both
           return game_time, box_score_url


class NHLScheduleRetriever(gm_utils.ScheduleRetriever):
    def __init__(self, source: gm_utils.ScheduleSource):
        super().__init__(source)

    async def retrieve(self, n_days: int = 1) -> None:
        # Get the URL for the NBA schedule
        url = gm_utils.get_url(self.source.name, 'schedule')
        # Use each date’s month to format URLs and fetch data
        for date in gm_utils.get_date_range(n_days, to_datetime=True):
            # format the url with the current season (2025) and month
            formatted_url = url.format(date.strftime("%Y%m%d"))
            # Asynchronously request the data and call parse schedule for each formatted URL
            await gm_utils.fetch(formatted_url, self._parse_schedule, date)

    async def _parse_schedule(self, html_content, date: datetime) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        table = soup.find('table')
        # extracts all rows except for the header row from the table
        rows = table.find_all('tr')[1:]
        # for each row
        for row in rows:
            # get the time and date of the game and check if it's in the right range of dates desired
            game_time, box_score_url = extract_game_time_and_box_score_url(row, date)
            # if the game time and box score url exist
            if game_time and box_score_url:
                # get the elements where team names lie
                span_elems = row.find_all('span', {'class': 'TeamName'})
                # make sure 2 teams exist
                if len(span_elems) > 1:
                    # get the away team name and id if it exists
                    if away_team := extract_team(span_elems[0], self.source.name, self.source.league):
                        # get the home team name and id if it exists
                        if home_team := extract_team(span_elems[1], self.source.name, self.source.league):
                            # adds the game and all of its extracted data to the shared data structure
                            self.update_games({
                                'time_processed': datetime.now(),
                                'source': self.source.name,
                                "league": self.source.league,
                                "game_time": game_time,
                                "away_team": away_team,
                                "home_team": home_team,
                                "box_score_url": box_score_url
                            })