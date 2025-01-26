from collections import defaultdict
from typing import Iterable

from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base_collection import BaseCollection


class BoxScores(BaseCollection):
    """
    A class to manage box scores in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The box scores collection.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the BoxScores class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__('box_scores', db)

    async def get_box_scores(self, query: dict) -> list[dict]:
        """
        Retrieves a list of box scores based on the given query.

        Args:
            query (dict): The query to find the box scores.

        Returns:
            list[dict]: A list of box score documents.
        """
        try:
            return await self.collection.find(query).to_list()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f"Failed to get box scores: {e}")

    async def get_box_score(self, query: dict) -> dict:
        """
        Retrieves a single box score document based on the given query.

        Args:
            query (dict): The query to find the box score.

        Returns:
            dict: The box score document.
        """
        try:
            return await self.collection.find_one(query)

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f"Failed to get box score: {e}")

    async def _update_box_score_doc(self, box_score: dict, matching_box_score_doc: dict):
        """
        Updates an existing box score document with new data.

        Args:
            box_score (dict): The new box score data.
            matching_box_score_doc (dict): The existing box score document to be updated.

        Returns:
            UpdateOne: The update operation to be performed on the database.

        Raises:
            Exception: If there is an error updating the box score document.
        """
        try:
            curr_period_num = int(box_score['game']['period'][0])
            matching_box_score = matching_box_score_doc['box_score']
            for stat_label, stat_value in box_score['box_score'].items():

                matching_box_score_stat_dict = matching_box_score[stat_label]
                matching_box_score_stat_periods = matching_box_score_stat_dict['periods']

                period_score_dict = { 'period': curr_period_num, 'stat': stat_value }
                if matching_box_score_stat_periods:
                    # for 2Q stats and beyond -- calculate period stats
                    period_score_dict['stat'] = stat_value - sum(
                        [period_stat_dict['stat'] for period_stat_dict in matching_box_score_stat_periods
                         if period_stat_dict['period'] < curr_period_num]
                    )
                    # update the period stat
                    if curr_period_num != matching_box_score_stat_periods[-1]['period']:
                        matching_box_score_stat_periods.append(period_score_dict)

                    else:
                        matching_box_score_stat_periods[-1] = period_score_dict
                else:
                    # for 1Q stats only
                    matching_box_score_stat_periods.insert(0, period_score_dict)

                # update the total stat
                matching_box_score_stat_dict['total'] = stat_value

            return await self.update_box_score(
                query={'_id': matching_box_score_doc['_id']},
                return_op=True,
                **matching_box_score_doc
            )

        except Exception as e:
            raise Exception(f"Failed to update box score doc: {box_score} {e}")

    @staticmethod
    def _create_box_score_doc(box_score: dict) -> dict:
        """
        Creates a new box score document from the given data.

        Args:
            box_score (dict): The box score data.

        Returns:
            dict: The new box score document.

        Raises:
            Exception: If there is an error creating the box score document.
        """
        try:
            game = box_score.pop('game')
            box_score['game_id'] = game['_id']
            # stores box score elements in lists for efficient aggregations later on
            new_box_score_doc = {
                **{key: value for key, value in box_score.items() if key != 'box_score'},
                'box_score': {
                    stat_label: { 'periods': [ {'period': game['period'][0], 'stat': stat_value } ], 'total': stat_value }
                    for stat_label, stat_value in box_score['box_score'].items()
                }
            }

            return new_box_score_doc

        except Exception as e:
            raise Exception(f"Failed to create box score doc: {box_score} {e}")

    async def store_box_scores(self, collected_boxscores: list[dict]) -> Iterable:
        """
        Stores a list of box scores in the database.

        Args:
            collected_boxscores (list[dict]): The list of box scores to store.
        """
        try:
            requests = defaultdict(list)
            for box_score in collected_boxscores:
                if matching_box_score_doc := await self.get_box_score({'_id': box_score['_id']}):
                    update_op = await self._update_box_score_doc(box_score, matching_box_score_doc)
                    requests['ops'].append(update_op)
                    requests['box_score_docs'].append(matching_box_score_doc)
                else:
                    new_box_score_doc = self._create_box_score_doc(box_score)
                    requests['ops'].append(InsertOne(new_box_score_doc))
                    requests['box_score_docs'].append(new_box_score_doc)

            await self.collection.bulk_write(requests['ops'])
            self.log_message(message=f"Successfully stored {len(collected_boxscores)} box scores", level='INFO')
            return requests['box_score_docs']

        except Exception as e:
            self.log_message(message=f"Failed to store box scores: {e}", level='EXCEPTION')

    async def update_box_score(self, query: dict, return_op: bool = False, **kwargs) -> UpdateOne | None:
        """
        Updates a box score in the database.

        Args:
            query (dict): The query to find the box score to update.
            return_op (bool): Whether to return the update operation.
            **kwargs: The fields to update.

        Returns:
            UpdateOne: The update operation if return_op is True.
        """
        try:
            if return_op:
                return UpdateOne(query, {'$set': kwargs})

            await self.collection.update_one(query, {'$set': kwargs})

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f"Failed to update box score: {e}")

    async def delete_box_scores(self, game_ids: list[str] = None):
        """
        Deletes box scores from the database.

        Args:
            game_ids (list[str], optional): The list of game IDs to filter by.
        """
        try:
            if game_ids:
                if await self.collection.delete_many({'game_id': {'$in': game_ids}}):
                    self.log_message(message=f"Successfully deleted box scores for games: {game_ids}", level='INFO')
                else:
                    raise Exception()

            else:
                if await self.collection.delete_many({}):
                    self.log_message(message="Successfully deleted all box scores", level='INFO')
                else:
                    raise Exception()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f"Failed to delete box scores: {e}")
