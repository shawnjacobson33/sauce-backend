import json
from collections import defaultdict
from datetime import datetime

import pymongo
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import InvalidOperation

from db.base_collection import BaseCollection
from db.collections.utils import GCSUploader
from db.collections.utils.stat_dicts import GameStatsDict, PlayerStatsDict


class BettingLines(BaseCollection):
    """
    A class to manage betting lines in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The betting lines collection.
        completed_betting_lines_collection (AsyncIOMotorDatabase.collection): The completed betting lines collection.
        gcs_uploader (GCSUploader): The Google Cloud Storage uploader.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the BettingLines class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__('betting_lines', db)

        self.completed_betting_lines_collection = self.db['completed_betting_lines']

        self.gcs_uploader = GCSUploader(bucket_name='betting-lines')

    async def get_betting_line(self, query: dict) -> dict:
        """
        Retrieves a single betting line document based on the given query.

        Args:
            query (dict): The query to find the betting line.

        Returns:
            dict: The betting line document.
        """
        try:
            return await self.collection.find_one(query)

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to get betting line: {e}')

    @staticmethod
    def _get_most_recent_stream_record(stream: list[dict]):
        """
        Retrieves the most recent record from the stream.

        Args:
            stream (list[dict]): The stream of records.

        Returns:
            dict: The most recent record.
        """
        most_recent_stream_record = stream[-1]
        for k, v in most_recent_stream_record.items():
            if isinstance(v, list):
                most_recent_stream_record[k] = v[-1]

        return most_recent_stream_record


    async def get_betting_lines(self, *args, **kwargs) -> list[dict]:
        """
        Retrieves betting lines based on the given arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            list[dict]: The betting lines.
        """
        try:
            if kwargs.pop('prev_batch', None):

                most_recent_betting_lines = []
                async for betting_line in self.collection.find(*args, **kwargs):
                    most_recent_betting_lines.append(
                        {**{k: v for k, v in betting_line.items() if k != 'stream'},
                         **self._get_most_recent_stream_record(betting_line['stream'])}
                    )

                return most_recent_betting_lines

            if kwargs.pop('completed', None):

                return await self.completed_betting_lines_collection.find(*args, **kwargs).to_list()

            return await self.collection.find({}).to_list()


        except Exception as e:
            raise Exception(f'Failed to get betting lines (args: {*args, kwargs}) : {e}')

    @staticmethod
    def _create_doc(line: dict) -> dict:
        """
        Creates a new betting line document.

        Args:
            line (dict): The betting line data.

        Returns:
            dict: The new betting line document.
        """
        record_fields = {'batch_timestamp', 'collection_timestamp', 'line', 'odds'}
        record = BettingLines._create_record(line)
        if not line['metrics'].get('ev'):
            record_fields.add('ev_formula')

        return {
            **{k: line[k] for k in line if k not in record_fields},
            'stream': [ record ],
        }

    @staticmethod
    def _create_record(line: dict) -> dict:
        """
        Creates a new record for the betting line stream.

        Args:
            line (dict): The betting line data.

        Returns:
            dict: The new record.
        """
        record = {
            'batch_timestamp': line['batch_timestamp'],
            'collection_timestamp': line['collection_timestamp'],
            'line': line['line'],
            'odds': line['odds'],
        }

        return record


    async def store_betting_lines(self, collected_betting_lines: list[dict]) -> int:
        """
        Stores betting lines in the database.

        Args:
            collected_betting_lines (list[dict]): The list of betting lines to store.
        """
        try:
            requests = []
            collected_betting_line_ids = []

            for collected_betting_line in collected_betting_lines:

                collected_betting_line_id = collected_betting_line['_id']
                collected_betting_line_ids.append(collected_betting_line_id)

                if stored_betting_line := await self.get_betting_line(collected_betting_line_id):

                    # update stream
                    stream = stored_betting_line['stream']
                    most_recent_record = stream[-1]
                    if (not (collected_betting_line['line'] == most_recent_record['line']) or
                            not (collected_betting_line['odds'] == most_recent_record['odds'])):

                        new_record = self._create_record(collected_betting_line)
                        stream.append(new_record)
                        update_op = await self.update_betting_line(collected_betting_line_id, return_op=True,
                                                                   stream=stream)  # Todo: do you need to replace the entire records field?
                        requests.append(update_op)

                    # update metrics if they changed
                    if stored_betting_line['metrics'] != collected_betting_line['metrics']:
                        update_op = await self.update_betting_line(collected_betting_line_id, return_op=True,
                                                                   metrics=collected_betting_line['metrics'])
                        requests.append(update_op)

                # insert a new doc
                new_betting_line_doc = self._create_doc(collected_betting_line)
                insert_op = pymongo.InsertOne(new_betting_line_doc)
                requests.append(insert_op)

            # if stored betting lines don't exist in the collected betting lines, update them
            stale_betting_lines = await self.get_betting_lines({'_id': {'$nin': collected_betting_line_ids}})
            for stale_betting_line in stale_betting_lines:

                stream = stale_betting_line['stream']
                most_recent_record = stream[-1]

                # Don't need to add an empty record consecutive times
                if len(most_recent_record) != 1:
                    stream.append({
                        'batch_timestamp': collected_betting_lines[0]['batch_timestamp'],
                    })
                    update_op = await self.update_betting_line(stale_betting_line['_id'], return_op=True,
                                                               stream=stream)
                    requests.append(update_op)

            await self.collection.bulk_write(requests)
            return 1

        except InvalidOperation as e:
            self.log_message(message=f'No betting lines to store: {e}', level='WARNING')

        except Exception as e:
            raise Exception(f'Failed to store betting lines: {e}')

    async def update_betting_line(self, betting_line_id: str, **kwargs):
        """
        Updates a betting line in the database.

        Args:
            betting_line_id (str): The unique ID of the betting line.
            **kwargs: The fields to update and along with other options.

        Returns:
            pymongo.UpdateOne: The update operation if return_op is True.
        """
        try:
            if kwargs.pop('return_op', False):
                return pymongo.UpdateOne({ '_id': betting_line_id }, { '$set': kwargs })

            await self.collection.update_one({ '_id': betting_line_id }, { '$set': kwargs })

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to update betting line: {e}')

    @staticmethod
    def _get_query_for_betting_lines_by_game(box_score_dict: dict) -> dict:
        try:
            return {
                'game._id': box_score_dict['game_id']
            }

        except Exception as e:
            raise Exception(f'Failed to get query for betting lines by subject: {e}')

    async def update_live_stats(self, updated_live_games: dict, stored_box_score_docs: list[dict]) -> None:
        """
        Updates live stats for betting lines based on the given box scores.

        Args:
            updated_live_games (list[dict]): The list of updated live games.
            stored_box_score_docs (list[dict]): The list of box scores.
        """
        try:
            # Todo: !!!!!!!  LEFT OFF HERE !!!!!!!
            requests = []
            seen_games = set()
            irrelevant_games = set()

            for stored_box_score_doc in stored_box_score_docs:

                game_id = stored_box_score_doc['game_id']
                if game_id not in seen_games:
                    query = self._get_query_for_betting_lines_by_game(stored_box_score_doc)
                    game_filtered_betting_lines = await self.get_betting_lines(query)

                    if not game_filtered_betting_lines:
                        seen_games.add(game_id)
                        irrelevant_games.add(game_id)
                        self.log_message(message=f'_update_live_stats(): no betting lines for game {game_id}', level='INFO')
                        continue

                    # update for game lines
                    game_stats_dict = GameStatsDict(updated_live_games[game_id])
                    game_lines_market_domain_betting_lines = [
                        betting_line for betting_line in game_filtered_betting_lines
                        if betting_line['market_domain'] == 'Gamelines'
                    ]

                    self.log_message(
                        message=f'_update_live_stats(): found {len(game_lines_market_domain_betting_lines)} game lines for game {game_id}'
                        , level='INFO')

                    missed_markets = set()
                    for betting_line in game_lines_market_domain_betting_lines:
                        if stat_value := game_stats_dict.get(betting_line['market'],
                                                             betting_line['subject']):

                            update_op = await self.update_betting_line(
                                betting_line['_id'], return_op=True, live_stat=stat_value
                            )
                            requests.append(update_op)
                        else:
                            missed_markets.add(betting_line['market'])

                    # update for player props
                    player_stats_dict = PlayerStatsDict(stored_box_score_doc['box_score'])
                    player_prop_lines_market_domain_betting_lines = [
                        betting_line for betting_line in game_filtered_betting_lines
                        if betting_line['market_domain'] == 'PlayerProps'
                    ]

                    self.log_message(
                        message=f'In _update_live_stats found {len(player_prop_lines_market_domain_betting_lines)} player prop lines for {game_id}',
                        level='INFO')

                    for betting_line in player_prop_lines_market_domain_betting_lines:
                        if stat_value := player_stats_dict.get(betting_line['market']):

                            update_op = await self.update_betting_line(
                                betting_line['_id'], return_op=True, live_stat=stat_value
                            )
                            requests.append(update_op)
                        else:
                            missed_markets.add(betting_line['market'])


                    if missed_markets:
                        self.log_message(
                            message=f'In _update_live_stats could not get stat for markets in game {game_id}: {missed_markets}',
                            level='WARNING')

                    seen_games.add(game_id)

            if irrelevant_games:
                self.log_message(level='INFO', message=f'No betting lines for: {irrelevant_games}')

            await self.collection.bulk_write(requests)

        except InvalidOperation as e:
            self.log_message(message=f'No live stats to update: {e}', level='WARNING')

        except Exception as e:
            self.log_message(message=f'Failed to update live stats: {e}', level='EXCEPTION')

    async def store_completed_betting_lines(self, game_ids: list[dict] = None, in_gcs: bool = False) -> None:
        """
        Stores completed betting lines in the database and optionally in Google Cloud Storage.

        Args:
            game_ids (list[dict], optional): The list of game IDs to filter by.
            in_gcs (bool, optional): Whether to store the betting lines in Google Cloud Storage.
        """
        # Todo: ANY FINAL STATS TO ADD?, OUTPUT THE SIZE OF THE JSON FILE
        try:
            if game_ids:
                optimized_projection = "get_projection_for_gcs()"
                betting_lines_filtered_by_game = await self.get_betting_lines({
                    'game._id': {'$in': game_ids},
                }, optimized_projection)
                if betting_lines_filtered_for_live_stat := [
                    # only want to store betting lines that have a final box score stat
                    betting_line for betting_line in betting_lines_filtered_by_game if betting_line.get('live_stat')
                ]:
                    await self.completed_betting_lines_collection.insert_many(
                        betting_lines_filtered_for_live_stat)

                else:
                    self.log_message(
                        message=f'store_completed_betting_lines(): No completed betting lines to store because there are no live stats: {game_ids}',
                        level='WARNING')

                # await self.delete_betting_lines(betting_lines_filtered_by_game)

            if in_gcs:
                # await self._store_in_gcs()
                await self.completed_betting_lines_collection.delete_many({})

        except Exception as e:
            self.log_message(message=f'Failed to store completed betting lines: {e}', level='EXCEPTION')

    async def delete_betting_lines(self, betting_lines: list[dict] = None) -> None:
        """
        Deletes betting lines from the database.

        Args:
            betting_lines (list[dict], optional): The list of betting lines to delete.
        """
        try:
            if betting_lines:
                betting_line_ids = [betting_line['_id'] for betting_line in betting_lines]
                if await self.collection.delete_many({ '_id': { '$in': betting_line_ids } }):
                    self.log_message(
                        message=f'Deleted {len(betting_line_ids)} betting lines: {betting_line_ids}',
                        level='INFO')
                else:
                    raise Exception()

            elif betting_lines is None:
                if await self.collection.delete_many({}):
                    self.log_message(message='Deleted all betting lines', level='INFO')
                else:
                    raise Exception()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to delete betting lines: {e}')
