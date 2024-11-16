from typing import Optional

from app.backend.data_collection.bookmakers import utils as bkm_utils


def extract_subject(cell, league: str, source_name: str) -> Optional[dict]:
    # get the link element
    if a_elem := cell.find('a'):
        # extract the player's name from the url link
        player_name = ' '.join(a_elem.get('href').split('/')[-2].split('-')).title()
        # get subject data from the shared data structure
        subject = bkm_utils.get_subject_id(source_name, league, player_name)
        # return the subject data
        return subject


def extract_basketball_stats(cells) -> Optional[dict]:
    # get all data from each cell element
    data = [cell.text for cell in cells]
    # make sure this player actually played
    if data[0] != '-':
        # return the statistical data
        return {
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
            'Minutes Played': int(data[7]),
            'Steals': int(data[8]),
            'Blocks': int(data[9]),
            'Turnovers': int(data[10]),
            'Plus Minus': data[11],
            'Fantasy Points': int(data[12])
        }


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
    data = (cell_comp for cell in cells for cell_comp in cell.split('-'))
    return {
            'Solo Tackles': int(next(data)),
            'Tackle Assists': int(next(data)),
            'Sacks': float(next(data)),
            'Interceptions': int(next(data)),
            'Forced Fumbles': int(next(data)),
        }


def extract_kicking_stats(cells) -> dict:
    data = (cell_comp for cell in cells for cell_comp in cell.split('/'))
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
            'Average Yards Per Punt': int(next(data)),
        }


FOOTBALL_TABLE_TYPE_MAP = {
    'passing': {'extract_func': extract_passing_stats, 'num_stats': 5},
    'rushing': {'extract_func': extract_rushing_stats, 'num_stats': 5},
    'receiving': {'extract_func': extract_receiving_stats, 'num_stats': 6},
    'defensive': {'extract_func': extract_defensive_stats, 'num_stats': 4},
    'kicking': {'extract_func': extract_kicking_stats, 'num_stats': 4},
    'punting': {'extract_func': extract_punting_stats, 'num_stats': 4},
}