from typing import Optional

import Levenshtein

from app import Subject
from app import clean_subject
from app import Subjects, ProblemData, RelevantData


subjects_df = Subjects.get_subjects(dtype='df')


def update_subject(subject: dict, matched_subject: dict):
    # update the subject with its id and optionally the team id stored if 'positions' was used to index
    subject['id'] = matched_subject['id']
    subject['team'] = matched_subject.get('team', subject.get('team'))


def find_match_using_distances(subject: dict) -> Optional[dict]:
    c_subject_name = clean_subject(subject['name'], subject['league'])
    f_subjects_df = subjects_df[subjects_df['league'] == subject['league']].copy()
    f_subjects_df['distance'] = f_subjects_df['name'].apply(lambda x: Levenshtein.distance(c_subject_name, x))
    closest_subject = f_subjects_df.sort_values(by='distance').iloc[0]
    if closest_subject['distance'] == 0:
        return {
            'id': closest_subject['id'],
            'name': closest_subject['name'],
            'team': closest_subject['team']
        }

# TODO: create a way to clean position for subjects where position data mismatches between bookmakers and cbs
def find_match(subject: dict) -> Optional[dict]:
    # attempt to find a stored subject
    if matched_subject := Subjects.get_subject(subject):
        # return if exists
        return matched_subject

    # otherwise use levehnstein distances
    return find_match_using_distances(subject)


def get_subject(source_name: str, league: str, name: str, **kwargs) -> Optional[dict[str, str]]:
    # clean the subject name
    c_subject_name = clean_subject(name, league)
    # create a dictionary representation of the subject
    subject = {key: value for key, value in Subject(c_subject_name, league, **kwargs).__dict__.items() if value}
    # DATA LOOKS LIKE --> {'id': 123asd, 'name': 'Jayson Tatum'} POSSIBLY WITH 'team': 'BOS'
    if matched_subject := find_match(subject):
        # update the subject's data
        update_subject(subject, matched_subject)
        # update the shared dictionary of relevant subjects only for bookmakers
        if 'cbssports' not in source_name:
            # update reporting data structure
            RelevantData.update_relevant_subjects(subject, source_name)

        # return the matched subject id and the actual name of the subject stored in the database
        return subject

    # update the shared dictionary of problem subjects
    ProblemData.update_problem_subjects(subject, source_name)



