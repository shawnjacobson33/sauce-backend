from datetime import datetime
from typing import Optional

from app.backend.data_collection import utils as dc_utils


def get_complete_date_time(date: datetime, time_str: str) -> Optional[datetime]:
    # Append "M" to the time string based on "a" or "p" suffix
    if time_str[-1].lower() in ['a', 'p']:
        time_str = time_str.strip() + "m"  # Add 'm' to make it "am" or "pm"

    try:
        # Parse and combine date with parsed time
        time_obj = datetime.strptime(time_str, "%I:%M %p")
        return date.replace(hour=time_obj.hour, minute=time_obj.minute)

    except ValueError:
        return None


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
           box_score_url = a_elem.get('href').split('/')[-2]
           # get the game time converted to a datetime object
           game_time = get_complete_date_time(date, a_elem.text)
           # return both
           return game_time, box_score_url


def is_box_score_url_valid(game: dict, url: str) -> None:
    # if 'scoreboard' is in the url that means there is no box score tracked for this game :(
    if 'scoreboard' not in url:
        # set the field to the valid url piece
        game['box_score_url'] = url
