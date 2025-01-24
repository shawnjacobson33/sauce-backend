import json
from collections import defaultdict
from datetime import datetime

import pymongo
from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.errors import InvalidOperation

from db.base_collection import BaseCollection
from db.collections.utils import GCSUploader


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

    def _get_cursor(self, *args, **kwargs):
        """
        Retrieves a cursor for the betting lines collection based on the given arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            AsyncIOMotorDatabase.cursor: The cursor for the betting lines collection.
        """
        if kwargs.pop('agg', None):
            return self.collection.aggregate(*args, **kwargs)
        elif n := kwargs.pop('n', None):
            return self.collection.find(*args, **kwargs).limit(n)

        return self.collection.find(*args, **kwargs)

    async def _get_most_recent_betting_lines(self, *args, **kwargs) -> list[dict]:
        """
        Retrieves the most recent betting lines based on the given arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            list[dict]: The most recent betting lines.
        """
        most_recent_betting_lines = []
        async for betting_line in self._get_cursor(*args, **kwargs):
            most_recent_stream_record = self._get_most_recent_stream_record(betting_line['stream'])
            most_recent_betting_lines.append(
                { **{k: v for k, v in betting_line.items() if k != 'stream'}, **most_recent_stream_record }
            )

        return most_recent_betting_lines

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
            if kwargs.pop('most_recent', None):
                return await self._get_most_recent_betting_lines(*args, **kwargs)

            return await self._get_cursor(*args, **kwargs).to_list()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to get betting lines: {e}')

    async def get_completed_betting_lines(self, *args, **kwargs) -> list[dict]:
        """
        Retrieves completed betting lines based on the given arguments.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            list[dict]: The completed betting lines.
        """
        try:
            return await self.completed_betting_lines_collection.find(*args, **kwargs).to_list()

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to get completed betting lines: {e}')

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

    async def _update_disappeared_betting_lines(self, seen_betting_lines: list[str], requests: list, batch_timestamp: str):
        """
        Updates betting lines that have disappeared.

        Args:
            seen_betting_lines (list[str]): The list of seen betting lines.
            requests (list): The list of update requests.
            batch_timestamp (str): The batch timestamp.
        """
        disappeared_betting_lines = await self.get_betting_lines(
            {'_id': {'$nin': seen_betting_lines}})
        for disappeared_betting_line in disappeared_betting_lines:
            stream = disappeared_betting_line['stream']
            most_recent_record = stream[-1]
            if len(most_recent_record) != 1:  # Don't need to add an empty record consecutive times
                stream.append({
                    'batch_timestamp': batch_timestamp,
                })
                update_op = await self.update_betting_line(disappeared_betting_line['_id'], return_op=True,
                                                           stream=stream)
                requests.append(update_op)

    async def _update_stream(self, stream: list[dict], new_betting_line: dict, requests: list):
        """
        Updates the stream of a betting line.

        Args:
            stream (list[dict]): The stream of records.
            new_betting_line (dict): The new betting line data.
            requests (list): The list of update requests.
        """
        most_recent_record = stream[-1]
        if (not (new_betting_line['line'] == most_recent_record.get('line')) or
                not (new_betting_line['odds'] == most_recent_record.get('odds'))):

            new_record = self._create_record(new_betting_line)
            stream.append(new_record)
            update_op = await self.update_betting_line(new_betting_line['_id'], return_op=True,
                                                       stream=stream)  # Todo: do you need to replace the entire records field?
            requests.append(update_op)

    async def store_betting_lines(self, betting_lines: list[dict]) -> None:
        """
        Stores betting lines in the database.

        Args:
            betting_lines (list[dict]): The list of betting lines to store.
        """
        try:
            requests = []
            betting_line_seen_tracker = defaultdict(int)
            for betting_line_dict in betting_lines:  # Todo: change so it loops through every betting line currently stored and then check if a betting line disappeared for this batch.
                betting_line_dict_id = betting_line_dict['_id']
                if betting_line_doc_match := await self.get_betting_line(betting_line_dict_id):
                    # update stream
                    stream = betting_line_doc_match['stream']
                    await self._update_stream(stream, betting_line_dict, requests)

                    # update metrics if they changed
                    if betting_line_doc_match['metrics'] != betting_line_dict['metrics']:
                        update_op = await self.update_betting_line(betting_line_dict_id, return_op=True,
                                                                   metrics=betting_line_dict['metrics'])
                        requests.append(update_op)


                elif betting_line_seen_tracker[betting_line_dict_id] == 0:  # For first iteration of batches
                    # insert a new doc
                    new_betting_line_doc = self._create_doc(betting_line_dict)
                    insert_op = pymongo.InsertOne(new_betting_line_doc)
                    requests.append(insert_op)

                betting_line_seen_tracker[betting_line_dict_id] += 1

            seen_betting_lines = list(betting_line_seen_tracker.keys())
            batch_timestamp = betting_lines[0]['batch_timestamp']
            # if betting lines disappeared, add an empty record to the stream
            await self._update_disappeared_betting_lines(seen_betting_lines, requests, batch_timestamp)

            await self.collection.bulk_write(requests)

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to store betting lines: {e}')

    async def update_betting_line(self, unique_id: str, return_op: bool = False, **kwargs):
        """
        Updates a betting line in the database.

        Args:
            unique_id (str): The unique ID of the betting line.
            return_op (bool): Whether to return the update operation.
            **kwargs: The fields to update.

        Returns:
            pymongo.UpdateOne: The update operation if return_op is True.
        """
        try:
            if return_op:
                return pymongo.UpdateOne({ '_id': unique_id }, { '$set': kwargs })

            await self.collection.update_one({ '_id': unique_id }, { '$set': kwargs })

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to update betting line: {e}')

    @staticmethod
    def _is_market_period_specific(market: str) -> bool:
        """
        Checks if the market is period-specific.

        Args:
            market (str): The market string.

        Returns:
            bool: True if the market is period-specific, False otherwise.
        """
        return market[:2] in {'1Q', '2Q', '3Q', '4Q'}

    def _get_box_score(self, betting_line: dict, box_score: dict) -> dict:
        """
        Retrieves the box score for a betting line.

        Args:
            betting_line (dict): The betting line data.
            box_score (dict): The box score data.

        Returns:
            dict: The box score for the betting line.
        """
        market = betting_line['market']
        if self._is_market_period_specific(market):
            return box_score[market[:2]]

        box_score = dict()
        periods = { '1H': {'1Q', '2Q'}, '2H': {'3Q', '4Q'} } # Todo: use ENUM?

        if market in periods:
            for specific_period in periods[market[:2]]:
                if period_specific_box_score := box_score.get(specific_period):
                    box_score.update(period_specific_box_score)

        for period_group in periods.values():
            for specific_period in period_group:
                if period_specific_box_score := box_score.get(specific_period):
                    box_score.update(period_specific_box_score)

        return box_score

    def _get_market(self, betting_line: dict) -> str:
        """
        Retrieves the market for a betting line.

        Args:
            betting_line (dict): The betting line data.

        Returns:
            str: The market string.
        """
        market = betting_line['market']
        return market[3:] if self._is_market_period_specific(market) else market

    async def update_live_stats(self, box_scores: list[dict]) -> None:
        """
        Updates live stats for betting lines based on the given box scores.

        Args:
            box_scores (list[dict]): The list of box scores.
        """
        try:
            requests = []
            for box_score_dict in box_scores:
                subject_filtered_betting_lines = await self.get_betting_lines({
                    'subject': box_score_dict['subject']['name'] , 'game._id': box_score_dict['game']['_id']
                })
                for betting_line in subject_filtered_betting_lines:
                    market = self._get_market(betting_line)
                    box_score = self._get_box_score(betting_line, box_score_dict)   # Todo: needs to be tested
                    if stat := box_score.get(market):
                        update_op = await self.update_betting_line(betting_line['_id'], return_op=True, live_stat=stat)
                        requests.append(update_op)

            await self.collection.bulk_write(requests)

        except InvalidOperation as e:
            self.log_message(message=f'No live stats to update: {e}', level='INFO')

        except Exception as e:
            self.log_message(message=f'Failed to update live stats: {e}', level='EXCEPTION')

    @staticmethod
    def _restructure_betting_lines_data(betting_lines: list[dict]):
        """
        Restructures betting lines data for storage.

        Args:
            betting_lines (list[dict]): The list of betting lines.
        """
        for betting_line in betting_lines:
            final_stat = betting_line.pop('live_stat', None)
            betting_line['final_stat'] = final_stat

            game = betting_line.pop('game')
            betting_line['game_id'] = game['_id']

    @staticmethod
    def _prepare_betting_lines_for_upload(betting_lines: list[dict]) -> str:
        """
        Prepares betting lines data for upload to Google Cloud Storage.

        Args:
            betting_lines (list[dict]): The list of betting lines.

        Returns:
            str: The betting lines data as a JSON string.
        """
        BettingLines._restructure_betting_lines_data(betting_lines)
        betting_lines_json = json.dumps(betting_lines)
        return betting_lines_json

    async def _store_in_gcs(self):
        """
        Stores completed betting lines in Google Cloud Storage.
        """
        try:
            completed_betting_lines = await self.get_completed_betting_lines({})
            betting_lines_json = self._prepare_betting_lines_for_upload(completed_betting_lines)
            blob_name = f"{datetime.now().strftime('%Y-%m-%d')}.json"
            self.gcs_uploader.upload(blob_name, betting_lines_json)

        except Exception as e:
            self.log_message(level='INFO', message=f'Failed to store completed betting lines in GCS: {e}')

    @staticmethod
    def _get_projection_for_gcs() -> dict:
        """
        Retrieves the projection for Google Cloud Storage upload.

        Returns:
            dict: The projection dictionary.
        """
        return {
            'url': 0, 'metrics': 0, 'subject': 0, 'bookmaker': 0, 'date': 0, 'label': 0,
            'league': 0, 'market': 0, 'market_domain': 0
        }

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
                optimized_projection = self._get_projection_for_gcs()
                betting_lines_filtered_by_game = await self.get_betting_lines({
                    'game._id': { '$in': game_ids },
                }, optimized_projection)
                betting_lines_filtered_for_live_stat = [ # only want to store betting lines that have a final box score stat
                    betting_line for betting_line in betting_lines_filtered_by_game if betting_line.get('live_stat')
                ]
                await self.completed_betting_lines_collection.insert_many(betting_lines_filtered_for_live_stat)
                await self.delete_betting_lines(betting_lines_filtered_by_game)

            if in_gcs:
                await self._store_in_gcs()
                await self.completed_betting_lines_collection.delete_many({})

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to store completed betting lines: {e}')

    async def delete_betting_lines(self, betting_lines: list[dict] = None) -> None:
        """
        Deletes betting lines from the database.

        Args:
            betting_lines (list[dict], optional): The list of betting lines to delete.
        """
        try:
            if betting_lines:
                betting_line_ids = [betting_line['_id'] for betting_line in betting_lines]
                await self.collection.delete_many({ '_id': { '$in': betting_line_ids } })

            await self.collection.delete_many({})

        except Exception as e:
            self.log_message(level='EXCEPTION', message=f'Failed to delete betting lines: {e}')
