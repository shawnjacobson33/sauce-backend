from typing import Optional

from app.backend.data_collection.utils import get_subject


def extract_team(div, game: dict) -> Optional[dict[str, str]]:
    # get the href link that holds the abbreviated team name
    if (a_elem := div.find('a')) and (href := a_elem.get('href')):
        # make sure there are enough elements to index
        if (len(href) > 3) and (team_name := href.split('/')[3]):
            # the team id is stored under the team's name
            return game.get(team_name)


def extract_position(span_elem, league: str) -> str:
    # get the position from element
    position = span_elem.text.strip()
    # if empty string and league is NHL
    if not position and league == 'NHL':
        # the player is a goalie
        return 'G'

    # otherwise just return found position
    return position


def extract_subject(cell, league: str, source_name: str, team: dict = None) -> Optional[dict]:
    # get the link element
    if (a_elem := cell.find('a')) and (href := a_elem.get('href')):
        # store the player's attribute in a dict
        attribute = {'team': team}
        # holds the position, football box scores also have a span element but no position
        if (span_elem := cell.find('span')) and league != 'NFL':
            # don't need two attributes
            del attribute['team']
            # get the player's position
            attribute['position'] = extract_position(span_elem, league)

        # extract the player's name from the url link
        player_name = ' '.join(href.split('/')[-2].split('-')).title()
        # get subject data from the shared data structure
        return get_subject(source_name, league, player_name, **attribute)


def extract_basketball_stats(cells, league: str) -> Optional[dict]:
    # get all data from each cell element
    data = [cell.text for cell in cells if 'for-mobile' not in cell.get('class')]
    # get the minutes played of this player
    minutes_played = data[7]
    # make sure this player actually played meaningful time
    if (minutes_played != '-') and (int(minutes_played) > 1):
        # return the statistical data
        box_score = {
            'Points': int(data[0]),
            'Rebounds': int(data[1]),
            'Assists': int(data[2]),
            'Field Goals Made': int(data[3].split('/')[0]),
            'Field Goals Attempted': int(data[3].split('/')[1]),
            '3-Pointers Made': int(data[4].split('/')[0]),
            '3-Pointers Attempted': int(data[4].split('/')[1]),
            'Free Throws Made': int(data[5].split('/')[0]),
            'Free Throws Attempted': int(data[5].split('/')[1]),
            'Personal Fouls': int(data[6]),
            'Minutes Played': int(minutes_played),
            'Steals': int(data[8]),
            'Blocks': int(data[9]),
            'Turnovers': int(data[10]),
        }

        # college basketball doesn't have these stats on cbssports
        if league == 'NBA':
            box_score['Plus Minus'] = data[11]
            box_score['Fantasy Points'] = int(data[12])

        return box_score


def extract_football_stats(cells, extraction_info: dict) -> dict:
    # get the box score for the give statistical type and subject
    return extraction_info['extract_func'](cells)


def extract_passing_stats(cells) -> dict:
    # TODO: Can't get Longest Passing Completion unless you go to the player's game log...extra request
    data = (cell_comp for cell in cells
            for slash_cell_comp in cell.text.split('/')
            for cell_comp in slash_cell_comp.split(':'))

    return {
                'Completions': int(next(data)),
                'Passing Attempts': int(next(data)),
                'Passing Yards': int(next(data)),
                'Passing Touchdowns': int(next(data)),
                'Interceptions Thrown': int(next(data)),
                'Fantasy Points': int(next(data))
            }


def extract_rushing_stats(cells) -> dict:
    data = (cell.text for cell in cells)
    return {
                'Carries': int(next(data)),
                'Rushing Yards': int(next(data)),
                'Rushing Touchdowns': int(next(data)),
                'Longest Rush': int(next(data)),
                'Fantasy Points': int(next(data)),
            }


def extract_receiving_stats(cells) -> dict:
    data = (cell.text for cell in cells)
    return {
            'Targets': int(next(data)),
            'Receptions': int(next(data)),
            'Receiving Yards': int(next(data)),
            'Receiving Touchdowns': int(next(data)),
            'Longest Reception': int(next(data)),
            'Fantasy Points': int(next(data)),
        }


def extract_defensive_stats(cells) -> dict:
    data = (cell_comp for cell in cells for cell_comp in cell.text.split('-'))
    return {
            'Solo Tackles': int(next(data)),
            'Tackle Assists': int(next(data)),
            'Sacks': float(next(data)),
            'Interceptions': int(next(data)),
            'Forced Fumbles': int(next(data)),
        }


def extract_kicking_stats(cells) -> dict:
    data = (cell_comp for cell in cells for cell_comp in cell.text.split('/'))
    return {
            'Field Goals Made': int(next(data)),
            'Field Goals Attempted': int(next(data)),
            'Longest Field Goal Made': int(next(data)),
            'Extra Points Made': int(next(data)),
            'Extra Points Attempted': int(next(data)),
            'Kicking Points': int(next(data)),
        }


def extract_punting_stats(cells) -> dict:
    data = (cell.text for cell in cells)
    return {
            'Punts': int(next(data)),
            'Average Yards Per Punt': float(next(data)),
        }


FOOTBALL_TABLE_TYPE_MAP = {
    'passing': {'extract_func': extract_passing_stats, 'num_cells': 15},
    'rushing': {'extract_func': extract_rushing_stats, 'num_cells': 5},
    'receiving': {'extract_func': extract_receiving_stats, 'num_cells': 6},
    'defense': {'extract_func': extract_defensive_stats, 'num_cells': 4},
    'kicking': {'extract_func': extract_kicking_stats, 'num_cells': 4},
    'punting': {'extract_func': extract_punting_stats, 'num_cells': 4},
}