import uuid
from collections import defaultdict
from typing import Optional

from pymongo import ReturnDocument

from app import UpdatedSubjects
from app import NewSubjects
from app import store_subject


visited_subjects = defaultdict(int)


def add_to_report(subject: dict, differences: dict) -> None:
    # store them in the updated_subjects data store
    UpdatedSubjects.update_store(subject, differences)
    # get the output strings that state what was updated in the subject
    update_reprs = [f"[{attr}]: {update_data['orig']} -> {update_data['new']}" for attr, update_data in
                    differences.items()]
    # give an output message
    print(f"[{subject['league']}]: updated {subject['name']} - {' '.join(update_reprs)}")


def find_differences(original_subject: dict, updated_subject: dict):
    attribute_differences = dict()
    # for every field in the subject object
    for key in original_subject:
        # if any of the original subject's values don't match the returned subject from update
        if original_subject[key] != updated_subject[key]:
            # update the differences dictionary with the new and original
            attribute_differences[key] = {
                'orig': original_subject[key],
                'new': updated_subject[key]
            }

    return attribute_differences


def delete_batch_id(subject_doc: dict):
    # check to see if the batch id field is in the document
    if 'batch_id' in subject_doc:
        # delete the batch id because it isn't relevant when comparing player attributes
        del subject_doc['batch_id']


def check_for_attr_differences(original_subject: Optional[dict], updated_subject: dict) -> None:
    # if it is a new subject than the original subject will be None
    if original_subject:
        # don't need the batch id when comparing player attributes
        delete_batch_id(original_subject), delete_batch_id(updated_subject)
        # find difference between subjects
        if differences := find_differences(original_subject, updated_subject):
            # report findings
            add_to_report(original_subject, differences)
    else:
        print(f"[{updated_subject['league']}]: inserted {updated_subject['name']}")


def add_to_stores(in_mem_store: defaultdict, subject: dict, new_subject: dict) -> None:
    # update the subject with return id
    subject['_id'] = str(new_subject['_id'])
    # update both data stores with the new subject
    store_subject(in_mem_store, subject)
    # update new subjects for reporting reasons
    NewSubjects.update_store(subject)


def attempt_to_update(collection, filter_condition: dict, subject: dict, batch_id: str) -> dict:
    # don't want to keep inserting duplicates/and game info can change...so perform an upsert using the filter
    return collection.find_one_and_update(
        filter_condition,
        {
            "$set": subject,
            "$setOnInsert": {'batch_id': batch_id}
        },
        upsert=True,
        return_document=ReturnDocument.AFTER
    )


def get_original_subject_doc(collection, filter_condition: dict) -> dict:
    # get the original subject document before updating
    return collection.find_one(filter_condition)


def prep_work(subject: tuple) -> tuple[str, dict]:
    global visited_subjects
    # add one for this subject's identifier
    visited_subjects[subject] += 1
    # create a new batch id to identify if a subject was inserted or not, create a unique identifier (primary key) to filter on
    return str(uuid.uuid4()), {
        'name': subject[2],
        'position': subject[1],
        'league': subject[0]
    }


def update_subjects(collection, in_mem_store: defaultdict, subject: dict) -> int:
    """ONLY USED BY ROSTER RETRIEVERS"""
    unique_identifier = (subject['league'], subject['position'], subject['name'])
    # if there is no other subject already looped over that has the same identifier
    if not visited_subjects[unique_identifier]:
        # do prep work to get batch id and filter condition, along with updating visited subjects
        batch_id, filter_condition = prep_work(unique_identifier)
        # get the original subject document
        original_subject = get_original_subject_doc(collection, filter_condition)
        # update a subject if a subject changes teams, jersey number, etc.
        returned_subject = attempt_to_update(collection, filter_condition, subject, batch_id)
        # if the returned subject document now has the batch id just created...then it was inserted
        if returned_subject.get('batch_id') == batch_id:
            # add the new subject to Subjects and NewSubjects data stores
            add_to_stores(in_mem_store, subject, returned_subject)
            # check to see if the subject had any new and then update the data store if so
            check_for_attr_differences(original_subject, returned_subject)
            # this represents a count for new subjects
            return 1

        # check to see if the subject had any new and then update the data store if so
        check_for_attr_differences(original_subject, returned_subject)

    # no new subjects
    return 0