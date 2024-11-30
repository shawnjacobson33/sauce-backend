import json
import os

from app.backend.data_collection.management.utils import reporting as rp
from app.backend.data_collection.workers import BettingLines, Games, BoxScores, Retriever, HUB_BOOKMAKERS


def report_line_counts(retriever: Retriever, time_taken: float) -> None:
    # Because OddsShopper isn't actually a bookmaker, but a tool that holds other bookmaker's odds
    if retriever.name in {'OddsShopper', 'PropProfessor'}:
        # for every bookmaker that they offer
        for bookmaker_name in HUB_BOOKMAKERS[retriever.name]:
            # output the amount of lines collected from each bookmaker they offer and the time taken for the whole job.
            print(f'[{bookmaker_name}]: {BettingLines.counts(bookmaker_name=bookmaker_name)}, {round(time_taken, 3)}s')
    else:
        # otherwise just output for the inputted bookmaker plug
        print(f'[{retriever.name}]: {retriever}, {round(time_taken, 3)}s')


def report_lines():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/betting_lines.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_data = rp.nest(BettingLines.get_lines())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_data, f, indent=4, default=str)

    # output the size of the file storing the betting lines
    print(f"[FILE SIZE]: {round(os.path.getsize('utils/reports/betting_lines.json') / (1024 ** 2), 2)} MB")


def report_games():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/games.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_data = rp.nest(Games.get_games())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_data, f, indent=4, default=str)


def report_box_scores():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/box_scores.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_data = rp.nest(BoxScores.get_box_scores())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_data, f, indent=4, default=str)


def generate_reports():
    report_games()
    report_lines()
    report_box_scores()