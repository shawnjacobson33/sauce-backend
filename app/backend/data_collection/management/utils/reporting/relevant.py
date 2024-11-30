import json

from app.backend.data_collection.workers import RelevantGames
from app.backend.data_collection.management.utils import reporting as rp
from app.backend.data_collection.management.utils.reporting.utils.stores import RelevantData


def report_relevant_leagues() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='leagues', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(RelevantData.get_relevant_leagues(), f, indent=4, default=str)


def report_relevant_games():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/relevant/games.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(RelevantGames.get_relevant_games(), f, indent=4, default=str)


def report_relevant_markets() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='markets', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_reports = rp.nest(RelevantData.get_relevant_teams())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_relevant_subjects() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='subjects', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_reports = rp.nest(RelevantData.get_relevant_teams())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_relevant_teams() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='teams', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # get rid of tuples
        restruct_reports = rp.nest(RelevantData.get_relevant_teams())
        # save the betting lines to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def generate_reports():
    report_relevant_games()
    report_relevant_teams()
    report_relevant_subjects()
    report_relevant_markets()
    report_relevant_leagues()
