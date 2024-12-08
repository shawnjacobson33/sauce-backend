import json

from app import RelevantGames, RelevantData
from app import utils as rp_utils


def report_relevant_leagues() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("leagues", is_secondary=True, secondary_type="relevant")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(RelevantData.get_relevant_leagues(), f, indent=4, default=str)


def report_relevant_games():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("games", is_secondary=True, secondary_type="relevant")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(RelevantGames.get_relevant_games(), f, indent=4, default=str)


def report_relevant_markets() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("markets", is_secondary=True, secondary_type="relevant")
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_reports = rp_utils.convert_deque_and_keys(RelevantData.get_relevant_markets())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_relevant_subjects() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("subjects", is_secondary=True)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_reports = rp_utils.convert_deque_and_keys(RelevantData.get_relevant_subjects())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_relevant_teams() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("teams", is_secondary=True)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_reports = rp_utils.convert_deque_and_keys(RelevantData.get_relevant_teams())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def generate_relevant_reports():
    report_relevant_games()
    report_relevant_teams()
    report_relevant_subjects()
    report_relevant_markets()
    report_relevant_leagues()
