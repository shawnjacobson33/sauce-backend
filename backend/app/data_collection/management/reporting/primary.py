import json
import os

from backend.app.data_collection.management.reporting import utils as rp_utils
from backend.app.data_collection.workers import Lines, Games, BoxScores, Retriever, HUB_BOOKMAKERS


def report_lines():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("lines")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # TODO: Find a way not to have to convert this everytime
        converted_data = rp_utils.convert_deque_and_keys(Lines.get_lines())
        # save the betting lines to the file, in pretty print mode
        json.dump(converted_data, f, indent=4, default=str)

    # output the size of the file storing the betting lines
    print(f"[FILE SIZE]: {round(os.path.getsize(file_path) / (1024 ** 2), 2)} MB")


def report_games():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("games")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_data = rp_utils.convert_deque_and_keys(Games.get_games())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_data, f, indent=4, default=str)


def report_box_scores():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("box_scores")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_data = rp_utils.convert_deque_and_keys(BoxScores.get_box_scores())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_data, f, indent=4, default=str)


def generate_primary_reports():
    report_games()
    report_lines()
    report_box_scores()