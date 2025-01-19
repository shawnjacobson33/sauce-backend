from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base import BaseCollection


class Subjects(BaseCollection):
    """
    A class to manage subjects in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The subjects collection.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the Subjects class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__(db)
        self.collection = self.db['subjects']

    async def get_subjects(self, query: dict) -> list[dict]:
        """
        Retrieves a list of subjects based on the given query.

        Args:
            query (dict): The query to find the subjects.

        Returns:
            list[dict]: A list of subject documents.
        """
        return await self.collection.find(query).to_list()

    async def get_subject(self, query: dict) -> dict:
        """
        Retrieves a single subject document based on the given query.

        Args:
            query (dict): The query to find the subject.

        Returns:
            dict: The subject document.
        """
        return await self.collection.find_one(query)

    async def store_subjects(self, subjects: list[dict]) -> None:
        """
        Stores a list of subjects in the database.

        Args:
            subjects (list[dict]): The list of subjects to store.
        """
        requests = []
        for subject in subjects:
            query = { 'name': subject['name'], 'jersey_number': subject['jersey_number'] }
            if await self.get_subject(query):
                update_op = await self.update_subject(query, return_op=True, **subject)
                requests.append(update_op)
            else:
                insert_op = InsertOne(subject)
                requests.append(insert_op)

        if requests:
            await self.collection.bulk_write(requests)

    async def update_subject(self, query: dict, return_op: bool = False, **kwargs):
        """
        Updates a subject in the database.

        Args:
            query (dict): The query to find the subject to update.
            return_op (bool): Whether to return the update operation.
            **kwargs: The fields to update.

        Returns:
            UpdateOne: The update operation if return_op is True.
        """
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_subjects(self, query: dict) -> None:
        """
        Deletes subjects from the database.

        Args:
            query (dict): The query to filter the subjects to delete.
        """
        await self.collection.delete_many(query)