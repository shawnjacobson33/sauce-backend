from typing import Optional

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers.utils.modelling import Subject


def filter_subject_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = dc_utils.Subjects.get_subjects()
    # filter it by partition
    return structured_data_store[league]


def update_subject(subject: dict, matched_subject: dict):
    # update the subject with its id and optionally the team id stored if 'positions' was used to index
    subject['id'] = matched_subject['id']
    subject['name'] = matched_subject['name']
    subject['team_id'] = matched_subject.get('team_id', subject.get('team_id'))


def get_subject(source_name: str, league: str, subject_name: str, attribute: dict) -> Optional[dict[str, str]]:
    # create a dictionary representation of the subject
    subject = {key: value for key, value in Subject(subject_name, league, **attribute).__dict__.items() if value}
    # filter by league partition
    if subjects := dc_utils.Subjects.get_subjects(league):
        # create an identifier based upon the attributes available
        if subjects := subjects.get(subject.get('position', subject.get('team_id'))):
            # get the matched data if it exists
            if matched_subject := subjects.get(dc_utils.clean_subject(subject['name'])):
                # update the subject's data
                update_subject(subject, matched_subject)
                # update the shared dictionary of relevant subjects only for bookmakers
                if 'cbssports' not in source_name:
                    dc_utils.RelevantData.update_relevant_subjects(subject, source_name, league)

                # return the matched subject id and the actual name of the subject stored in the database
                return subject

    # update the shared dictionary of problem subjects
    dc_utils.ProblemData.update_problem_subjects(subject, source_name, league)



