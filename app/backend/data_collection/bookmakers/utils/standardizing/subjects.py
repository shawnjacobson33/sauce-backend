from typing import Optional

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers.utils.modelling import Subject
from app.backend.data_collection.bookmakers.utils.cleaning import clean_subject


def filter_subject_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = dc_utils.Subjects.get_subjects()
    # filter it by partition
    return structured_data_store[league]


def get_subject_id(source_name: str, league: str, subject_name: str, **kwargs) -> Optional[dict[str, str]]:
    # create a subject object
    subject = Subject(subject_name, league, **kwargs)
    # clean the subject name
    if cleaned_subject := clean_subject(subject.name):
        # filter by league partition
        filtered_subjects = filter_subject_data(subject.league)
        # get the matched data if it exists
        if matched_subject := filtered_subjects.get(cleaned_subject):
            # add the mapped name to the matched data dictionary
            matched_subject['name'] = cleaned_subject
            # update the shared dictionary of valid subjects
            dc_utils.RelevantData.update_relevant_subjects(matched_subject, source_name, league)
            # return the matched subject id and the actual name of the subject stored in the database
            return matched_subject

    # get all the attributes of the subject not found in the database that are not null
    subject_dict = {key: value for key, value in subject.__dict__.items() if value}
    # update the shared dictionary of problem subjects
    dc_utils.ProblemData.update_problem_subjects(subject_dict, source_name, league)



