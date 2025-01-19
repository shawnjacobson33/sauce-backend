from datetime import datetime

from pymongo import UpdateOne, InsertOne
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base import BaseCollection


class Games(BaseCollection):
    """
    A class to manage games in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The games collection.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the Games class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__(db)
        self.collection = self.db['games']

    async def get_games(self, query: dict, live: bool = False) -> list[dict]:
        """
        Retrieves a list of games based on the given query.

        Args:
            query (dict): The query to find the games.
            live (bool): Whether to filter for live games.

        Returns:
            list[dict]: A list of game documents.
        """
        if live:
            live_query = {
                **query,
                '$or': [
                    {'status': 'live'},
                    {'game_time': {'$lte': datetime.now()}}
                ]
            }
            return await self.collection.find(live_query).to_list()

        return await self.collection.find(query).to_list()

    async def get_game(self, query: dict, proj: dict = None) -> dict:
        """
        Retrieves a single game document based on the given query.

        Args:
            query (dict): The query to find the game.
            proj (dict, optional): The projection to apply to the query.

        Returns:
            dict: The game document.
        """
        return await self.collection.find_one(query, proj if proj else {})

    async def store_games(self, games: list[dict]) -> None:
        """
        Stores a list of games in the database.

        Args:
            games (list[dict]): The list of games to store.
        """
        requests = []
        for game in games:
            query = { '_id': game['_id'] }
            if await self.get_game(query):
                update_op = await self.update_game(query, return_op=True, **game)
                requests.append(update_op)
            else:
                insert_op = InsertOne(game)
                requests.append(insert_op)

        if requests:
            await self.collection.bulk_write(requests)

    async def update_game(self, query: dict, return_op: bool = False, **kwargs):
        """
        Updates a game in the database.

        Args:
            query (dict): The query to find the game to update.
            return_op (bool): Whether to return the update operation.
            **kwargs: The fields to update.

        Returns:
            UpdateOne: The update operation if return_op is True.
        """
        if return_op:
            return UpdateOne(query, {'$set': kwargs})

        await self.collection.update_one(query, {'$set': kwargs})

    async def delete_games(self, game_ids: list[str] = None) -> None:
        """
        Deletes games from the database.

        Args:
            game_ids (list[str], optional): The list of game IDs to filter by.
        """
        if game_ids:
            return await self.collection.delete_many({'_id': {'$in': game_ids}})

        await self.collection.delete_many({})
