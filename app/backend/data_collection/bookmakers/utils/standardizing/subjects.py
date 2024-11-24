from typing import Optional

import Levenshtein

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers.utils.modelling import Subject
from app.backend.data_collection.utils import clean_subject


subjects = dc_utils.Subjects.get_store()
subjects_df = dc_utils.Subjects.get_store(dtype='df')


def update_subject(subject: dict, matched_subject: dict):
    # update the subject with its id and optionally the team id stored if 'positions' was used to index
    subject['id'] = matched_subject['id']
    subject['team_id'] = matched_subject.get('team_id', subject.get('team_id'))


def find_match_using_distances(subject: dict) -> Optional[dict]:
    c_subject_name = clean_subject(subject['name'], subject['league'])
    f_subjects_df = subjects_df[subjects_df['league'] == subject['league']].copy()
    f_subjects_df['distance'] = f_subjects_df['name'].apply(lambda x: Levenshtein.distance(c_subject_name, x))
    closest_subject = f_subjects_df.sort_values(by='distance').iloc[0]
    if not closest_subject['distance']:
        return {
            'id': closest_subject['id'],
            'name': closest_subject['name'],
            'team_id': closest_subject['team_id']
        }


def find_match(subject: dict) -> Optional[dict]:
    # get subjects for a league
    if league_data := subjects.get(subject['league']):
        # get subjects for an attribute, if source gave an attribute
        if (attr := subject.get('position', subject.get('team_id'))) and (attr_data := league_data.get(attr)):
            # TODO: create a way to clean position for subjects where position data mismatches between bookmakers and cbs
            # return the attempted match
            return attr_data.get(clean_subject(subject['name'], subject['league']))

        # otherwise use levehnstein distances
        return find_match_using_distances(subject)


def get_subject(source_name: str, league: str, name: str, **kwargs) -> Optional[dict[str, str]]:
    # create a dictionary representation of the subject
    subject = {key: value for key, value in Subject(name, league, **kwargs).__dict__.items() if value}
    # get the matched data if it exists
    if matched_subject := find_match(subject):
        # update the subject's data
        update_subject(subject, matched_subject)
        # update the shared dictionary of relevant subjects only for bookmakers
        if 'cbssports' not in source_name:
            # update reporting data structure
            dc_utils.RelevantData.update_relevant_subjects(subject, source_name, league)

        # return the matched subject id and the actual name of the subject stored in the database
        return subject

    # update the shared dictionary of problem subjects
    dc_utils.ProblemData.update_problem_subjects(subject, source_name, league)



