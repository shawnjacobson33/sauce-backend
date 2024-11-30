import json

from app.backend.data_collection.management.utils import reporting as rp
from app.backend.data_collection.management.utils.reporting.utils.stores import ProblemData


def report_problem_markets() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='markets', is_pending=True)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # to get rid of tuples
        restruct_reports = rp.nest(ProblemData.get_problem_markets())
        # save the pending subjects to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_problem_subjects():
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='subjects', is_pending=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # to get rid of tuples
        restruct_reports = rp.nest(ProblemData.get_problem_subjects())
        # save the pending subjects to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_problem_teams():
    # create a custom file path to store the betting lines sample
    file_path = rp.get_file_path(entity_type='teams', is_pending=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # to get rid of tuples
        restruct_reports = rp.nest(ProblemData.get_problem_teams())
        # save the pending subjects to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def generate_reports():
    report_problem_markets()
    report_problem_teams()
    report_problem_subjects()
