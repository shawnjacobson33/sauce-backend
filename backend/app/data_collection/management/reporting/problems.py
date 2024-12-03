import json

from app.backend.data_collection.workers import ProblemData
from app.backend.data_collection.management.reporting import utils as rp_utils


def report_problem_markets() -> None:
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("markets", is_secondary=True, is_problem=True)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # to get rid of tuples
        restruct_reports = rp_utils.nest(ProblemData.get_problem_markets())
        # save the pending subjects to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_problem_subjects():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("subjects", is_secondary=True, is_problem=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # to get rid of tuples
        restruct_reports = rp_utils.nest(ProblemData.get_problem_subjects())
        # save the pending subjects to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def report_problem_teams():
    # create a custom file path to store the betting lines sample
    file_path = rp_utils.get_file_path("teams", is_secondary=True, is_problem=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # to get rid of tuples
        restruct_reports = rp_utils.nest(ProblemData.get_problem_teams())
        # save the pending subjects to the file, in pretty print mode
        json.dump(restruct_reports, f, indent=4, default=str)


def generate_problem_reports():
    report_problem_markets()
    report_problem_teams()
    report_problem_subjects()
