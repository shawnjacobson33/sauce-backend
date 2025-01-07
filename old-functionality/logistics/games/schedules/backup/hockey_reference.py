from typing import Optional, Union, Any

from bs4 import BeautifulSoup

from app import utils as dc_utils
from app import utils as gm_utils


# TODO: Same as NFL
def convert_to_datetime(date_str, time_str) -> datetime:
    # Parse the date string
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    # Parse the time string with AM/PM format
    time_obj = datetime.strptime(time_str, "%I:%M %p")
    # Combine the parsed date with the parsed time
    result = date_obj.replace(hour=time_obj.hour, minute=time_obj.minute, second=0, microsecond=0)
    # return the result
    return result


def extract_data(tr_elem, attr_name: str) -> str | None:
    # extracts any text from a data cell in the row for a given attribute name
    if td_elem := tr_elem.find('td', {'data-stat': attr_name}):
        return td_elem.text


def extract_game_date_and_box_score_url(tr_elem) -> Union[tuple[Any, Any], tuple[None, None]]:
    # extracts the date from the row element if it exists
    if th_elem := tr_elem.find('th', {'data-stat': 'date_game'}):
        # get the link holding box score url
        if box_score_identifier := th_elem.get('csk'):
            # return both if both exist
            return th_elem.text, f'/boxscores/{box_score_identifier}.html'

        # otherwise just return game date
        return th_elem.text, None

    return None, None


def extract_game_time_and_box_score_url(row, valid_dates: list[str]) -> Optional[dict[str, str]]:
    # get the game date and box score url
    game_date, box_score_url = extract_game_date_and_box_score_url(row)
    # get the game date and start time of the game if it exists
    if game_date and (start_time := extract_data(row, 'time_game')):
        # convert it to a comparable datetime object
        game_datetime = convert_to_datetime(game_date, start_time)
        # make sure that this game is in the desired date range
        if game_datetime.strftime("%Y-%m-%d") in valid_dates:
            # convert it to a comparable datetime object and then cast to a string, also return box score url
            return {
                'game_time': game_datetime,
                'box_score_url': box_score_url
            }


def extract_team(source_name: str, league: str, row, attr_name: str) -> Optional[dict[str, str]]:
    # get the team name from the html
     if team_name := extract_data(row, attr_name):
         # get the team id and team name from the database
         if team_data := dc_utils.get_team(source_name, league, team_name):
             # return the team id and team name
             return team_data


class NHLScheduleCollector(sc_utils.ScheduleRetriever):
    def __init__(self, source: gm_utils.GameSource):
        super().__init__(source)

    async def retrieve(self, n_days: int = 1) -> None:
        # generate a range of dates predicated upon n_days param
        date_list = gm_utils.get_date_range(n_days)
        # get the url for hockey-reference.com's nhl schedule
        url = lg_utils.get_url(self.source, 'schedule')
        # format the url with the season
        formatted_url = url.format(gm_utils.CURR_SEASON_1)
        # asynchronously request the data and call parse schedule
        await lg_utils.fetch(formatted_url, self._parse_schedule, date_list)

    async def _parse_schedule(self, html_content, date_list: list[str]) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # extracts the table element that holds schedule data
        table = soup.find('table', {'id': 'games'})
        # extracts all rows except for the header row from the table
        rows = table.find_all('tr')[1:]
        # for each row
        for row in rows:
            # get both game time formatted and box score url
            if game_time_data := extract_game_time_and_box_score_url(row, date_list):
                # get the away team name and id if it exists
                if away_team_id := extract_team(self.name, self.league, row,
                                             'visitor_team_name'):
                    # get the home team name and id if it exists
                    if home_team_id := extract_team(self.name, self.league, row,
                                                 'home_team_name'):
                        # adds the game and all of its extracted data to the shared data structure
                        self.update_games({
                            's_tstamp': str(datetime.now()),
                            "league": self.league,
                            "game_time": game_time_data['game_time'],
                            "away_team": away_team,
                            "home_team": home_team,
                            "box_score_url": game_time_data['box_score_url'],
                            "game_notes": extract_data(row, 'game_remarks')  # TODO: Same as NBA
                        })
