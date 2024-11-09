import asyncio
import pprint
from datetime import datetime
from typing import Optional

from bs4 import BeautifulSoup

from app.data_collection.games import utils as gm_utils


def is_date_today(date_str: str) -> bool:
    # Parse the input date string into a datetime object
    input_date = datetime.strptime(date_str.strip(), "%A, %B %d, %Y").date()
    # Get today's date
    today = datetime.today().date()
    # return a bool checking whether the dates are equal
    return input_date == today


def clean_team(team_name: str) -> str:
    # Find the index of the first letter
    first_letter_index = next((i for i, char in enumerate(team_name) if char.isalpha()), -1)
    # return the cleaned team name
    return team_name[first_letter_index:]


def convert_to_date(time_str: str) -> str:
    # Get today's date
    today_date = datetime.today().date()
    # Combine today's date with the time string
    return str(datetime.strptime(f"{today_date} {time_str}", "%Y-%m-%d %I:%M %p"))


def extract_game_time(cell) -> Optional[str]:
    if a_elem := cell.find('a'):
        return convert_to_date(a_elem.text)


class NCAAFScheduleCollector(gm_utils.ScheduleCollector):
    def __init__(self, source_info: gm_utils.Source):
        super().__init__(source_info)

    async def collect(self) -> None:
        # get the url for cbssports.com's ncaaf schedules
        url = gm_utils.get_url(self.source_info.name, self.source_info.league, 'schedule')
        # asynchronously request the data and call parse schedule
        await gm_utils.fetch(url, self._parse_games)

    async def _parse_games(self, html_content) -> None:
        # initializes a html parser
        soup = BeautifulSoup(html_content, 'html.parser')
        # get all of the divs that contain all rows
        div_elems = soup.find_all('div', {'class': 'TableBaseWrapper'})
        # for each div
        for div_elem in div_elems:
            # check if the h4 element contains the correct date for today
            if (h4_elem := div_elem.find('h4')) and (is_date_today(h4_elem.text)):
                # get all the rows (games) for today
                if (rows := div_elem.find_all('tr')) and (len(rows) > 1):
                    # for each row not including the header
                    for row in rows[1:]:
                        # get all the cells for the game
                        if (cells := row.find_all('td')) and (len(cells) > 2):
                            # extract the away team and home team if they exist
                            if (away_team := cells[0].text) and (home_team := cells[1].text):
                                # clean the team names
                                away_team, home_team = clean_team(away_team), clean_team(home_team)
                                # extract the game time
                                if game_time := extract_game_time(cells[2]):
                                    # adds the game and all of its extracted data to the shared data structure
                                    self.update_games({
                                        'time_processed': str(datetime.now()),
                                        "game_time": game_time,
                                        "league": self.source_info.league,
                                        "away_team": away_team,
                                        "home_team": home_team
                                    })

                # only need one date for now
                break


async def main():
    from app.data_collection.games.utils.shared_data import Games
    source = gm_utils.Source('CBSSPORTS', 'NCAAF')
    await NCAAFScheduleCollector(source).collect()
    pprint.pprint(Games.get_games())

if __name__ == '__main__':
    asyncio.run(main())
