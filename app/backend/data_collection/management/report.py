from app.backend.data_collection.management import utils as mg_utils


def generate_reports():
    mg_utils.generate_primary_reports()
    mg_utils.generate_problem_reports()
    mg_utils.generate_relevant_reports()