from typing import Optional

from app.backend.data_collection.bookmakers.utils.modelling import Subject
from app.backend.data_collection.bookmakers.utils.shared_data import Subjects
from app.backend.data_collection.bookmakers.utils.cleaning import clean_subject


def filter_subject_data(league: str) -> dict:
    # get the data structured as dictionary or a dataframe based upon the input
    structured_data_store = Subjects.get_stored_data()
    # filter it by partition
    return structured_data_store[league]


def get_subject_id(bookmaker_name: str, league: str, subject_name: str, **kwargs) -> Optional[dict[str, str]]:
    # create a subject object
    subject = Subject(subject_name, league, **kwargs)
    # clean the subject name
    if cleaned_subject := clean_subject(subject.name):
        # filter by league partition
        filtered_data = filter_subject_data(subject.league)
        # get the matched data if it exists
        if matched_data := filtered_data.get(cleaned_subject):
            # cast the matched id to a string
            matched_data['id'] = str(matched_data['id'])
            # update the shared dictionary of valid subjects
            Subjects.update_valid_data(bookmaker_name, tuple(matched_data.items()))
            # return the matched subject id and the actual name of the subject stored in the database
            return {
                'id': matched_data['id'],
                'name': matched_data['name']
            }

    # update the shared dictionary of pending subjects
    Subjects.update_pending_data(bookmaker_name, tuple(subject.__dict__.items()))



