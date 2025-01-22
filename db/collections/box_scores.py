from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base import BaseCollection


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
        self.collection = self.db['box_scores']

    async def get_box_scores(self, query: dict) -> list[dict]:
        """
        Retrieves a list of box scores based on the given query.

        Args:
            query (dict): The query to find the box scores.

        Returns:
            list[dict]: A list of box score documents.
        """
        return await self.collection.find(query).to_list()

    async def get_box_score(self, query: dict) -> dict:
        """
        Retrieves a single box score document based on the given query.

        Args:
            query (dict): The query to find the box score.

        Returns:
            dict: The box score document.
        """
        return await self.collection.find_one(query)

    async def store_box_scores(self, box_scores: list[dict]) -> None:
        """
        Stores a list of box scores in the database.

        Args:
            box_scores (list[dict]): The list of box scores to store.
        """
        requests = []
        for box_score in box_scores:
            query = {'_id': box_score['_id']}
            if await self.get_box_score(query):
                update_op = await self.update_box_score(query, return_op=True, **box_score)
                requests.append(update_op)
            else:
                insert_op = InsertOne(box_score)
                requests.append(insert_op)

        if requests:
            await self.collection.bulk_write(requests)

    async def update_box_score(self, query: dict, return_op: bool = False, **kwargs):
        """
        Updates a box score in the database.

        Args:
            query (dict): The query to find the box score to update.
            return_op (bool): Whether to return the update operation.
            **kwargs: The fields to update.

        Returns:
            UpdateOne: The update operation if return_op is True.
        """
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_box_scores(self, game_ids: list[str] = None):
        """
        Deletes box scores from the database.

        Args:
            game_ids (list[str], optional): The list of game IDs to filter by.
        """
        if game_ids:
            return await self.collection.delete_many({'game._id': {'$in': game_ids}})

        await self.collection.delete_many({})
