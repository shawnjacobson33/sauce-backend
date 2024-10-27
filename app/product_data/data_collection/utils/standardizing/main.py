from typing import Optional

from app.product_data.data_collection.utils.objects import Subject, Market
from app.product_data.data_collection.utils.standardizing.constants import UPDATE_DATABASE_IS_ALLOWED
from app.product_data.data_collection.utils.standardizing.helpers import first_search, second_search, \
    insert_new_entity, output_msg


def get_market_id(market: Market, user: str = None) -> Optional[str]:
    # check the in-memory dictionary for a match
    if market_id := first_search(market, user):
        return market_id

    # see if there is a stored market very similar (indication of a match) to this market
    if market_id := second_search(market, n_entities=1):
        return market_id

    # in a production environment probably don't want to risk database invalidity.
    if UPDATE_DATABASE_IS_ALLOWED:
        return insert_new_entity(market)

    # only applicable when database updates aren't allowed
    output_msg(market, msg_type='not_found')


def get_subject_id(subject: Subject, user: str = None):
    # check the in-memory dictionary for a match
    if subject_id := first_search(subject, user):
        return subject_id

    # see if there is a stored market very similar (indication of a match) to this market
    if subject_id := second_search(subject, n_entities=1):
        return subject_id

    # in a production environment probably don't want to risk database invalidity.
    if UPDATE_DATABASE_IS_ALLOWED:
        return insert_new_entity(subject)

    # only applicable when database updates aren't allowed
    output_msg(subject, msg_type='not_found')
