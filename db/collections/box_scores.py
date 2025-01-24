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
        try:
            curr_period_num = int(box_score['game']['period'][0])
            matching_box_score = matching_box_score_doc['box_score']
            for stat_label, stat_value in box_score['box_score'].items():
                matching_box_score_stat_dict = matching_box_score[stat_label]
                matching_box_score_stat_records = matching_box_score_stat_dict['records']
                if len(matching_box_score_stat_records) > 1:
                    # for 2Q stats and beyond -- calculate period stats
                    period_stat = stat_value - sum([period_stat for period_stat in matching_box_score_stat_records])
                    # update the period stat
                    if curr_period_num > len(matching_box_score_stat_records):
                        matching_box_score_stat_records.append(period_stat)
                    else:
                        matching_box_score_stat_records[curr_period_num-1] = period_stat
                else:
                    # for 1Q stats only
                    matching_box_score_stat_records[0] = stat_value

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
        try:
            box_score['game_id'] = box_score.pop('game')['_id']
            # stores box score elements in lists for efficient aggregations later on
            new_box_score_doc = {
                **{key: value for key, value in box_score.items() if key != 'box_score'},
                'box_score': {
                    stat_label: [stat_value] for stat_label, stat_value in box_score['box_score'].items()
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
                return await self.collection.delete_many({'game_id': {'$in': game_ids}})

            await self.collection.delete_many({})

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f"Failed to delete box scores: {e}")
