from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base_collection import BaseCollection


class Teams(BaseCollection):
    """
    A class to manage teams in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The teams collection.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the Teams class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__('teams', db)

    async def get_teams(self, query: dict) -> list[dict]:
        """
        Retrieves a list of teams based on the given query.

        Args:
            query (dict): The query to find the teams.

        Returns:
            list[dict]: A list of team documents.
        """
        try:
            return await self.collection.find(query, { '_id': 0 }).to_list()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to get teams: {e}')

    async def get_team(self, query: dict) -> dict:
        """
        Retrieves a single team document based on the given query.

        Args:
            query (dict): The query to find the team.

        Returns:
            dict: The team document.
        """
        try:
            return await self.collection.find_one(query)

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to get team: {e}')

    async def store_teams(self, teams: list[dict]) -> None:
        """
        Stores a list of teams in the database.

        Args:
            teams (list[dict]): The list of teams to store.
        """
        try:
            requests = []
            for team in teams:
                query = {'full_name': team['full_name']}
                if await self.get_team(query):
                    update_op = await self.update_team(query, return_op=True, **team)
                    requests.append(update_op)
                else:
                    insert_op = InsertOne(team)
                    requests.append(insert_op)

            await self.collection.bulk_write(requests)

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to store teams: {e}')

    async def update_team(self, query: dict, return_op: bool = False, **kwargs):
        """
        Updates a team in the database.

        Args:
            query (dict): The query to find the team to update.
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
            self.log_message(level='EXCEPTION', message=f'Failed to update team: {e}')

    async def delete_teams(self, query: dict) -> None:
        """
        Deletes teams from the database.

        Args:
            query (dict): The query to filter the teams to delete.
        """
        try:
            await self.collection.delete_many(query)

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to delete teams: {e}')