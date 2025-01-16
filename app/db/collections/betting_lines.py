import json
from collections import defaultdict
from datetime import datetime

import pymongo
from motor.motor_asyncio import AsyncIOMotorDatabase
from google.cloud import storage

from app.db.base import BaseCollection


class BettingLines(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['betting_lines']

    async def get_betting_line(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    @staticmethod
    def _get_most_recent_stream_record(stream: list[dict]):
        most_recent_stream_record = stream[-1]
        for k, v in most_recent_stream_record.items():
            if isinstance(v, list):
                most_recent_stream_record[k] = v[-1]

        return most_recent_stream_record

    async def _get_most_recent_betting_lines(self, query: dict) -> list[dict]:
        most_recent_betting_lines = []
        async for betting_line in self.collection.find(query, {'stream': { '$slice': -1 }}):
            most_recent_stream_record = self._get_most_recent_stream_record(betting_line['stream'])
            most_recent_betting_lines.append(
                { **{k: v for k, v in betting_line.items() if k != 'stream'}, **most_recent_stream_record }
            )

        return most_recent_betting_lines

    async def get_betting_lines(self, query: dict, most_recent: bool = False) -> list[dict]:
        if most_recent:
            return await self._get_most_recent_betting_lines(query)

        return await self.collection.find(query).to_list()

    @staticmethod
    def _create_doc(line: dict) -> dict:
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
        record = {
            'batch_timestamp': line['batch_timestamp'],
            'collection_timestamp': line['collection_timestamp'],
            'line': line['line'],
            'odds': line['odds'],
        }

        return record

    async def _update_disappeared_betting_lines(self, seen_betting_lines: list[str], requests: list, batch_timestamp: str):
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
        most_recent_record = stream[-1]
        if (not (new_betting_line['line'] == most_recent_record.get('line')) or
                not (new_betting_line['odds'] == most_recent_record.get('odds'))):

            new_record = self._create_record(new_betting_line)
            stream.append(new_record)
            update_op = await self.update_betting_line(new_betting_line['_id'], return_op=True,
                                                       stream=stream)  # Todo: do you need to replace the entire records field?
            requests.append(update_op)

    async def store_betting_lines(self, betting_lines: list[dict]) -> None:
        requests = []
        betting_line_seen_tracker = defaultdict(int)
        for betting_line_dict in betting_lines:  # Todo: change so it loops through every betting line currently stored and then check if a betting line disappeared for this batch.
            betting_line_dict_id = betting_line_dict['_id']
            if betting_line_doc_match := await self.get_betting_line(betting_line_dict_id):
                stream = betting_line_doc_match['stream']
                await self._update_stream(stream, betting_line_dict, requests)

            elif betting_line_seen_tracker[betting_line_dict_id] == 0:  # For first iteration of batches
                new_betting_line_doc = self._create_doc(betting_line_dict)
                insert_op = pymongo.InsertOne(new_betting_line_doc)
                requests.append(insert_op)

            betting_line_seen_tracker[betting_line_dict_id] += 1

        seen_betting_lines = list(betting_line_seen_tracker.keys())
        batch_timestamp = betting_lines[0]['batch_timestamp']
        await self._update_disappeared_betting_lines(seen_betting_lines, requests, batch_timestamp)

        if requests:
            await self.collection.bulk_write(requests)

    async def update_betting_line(self, unique_id: str, return_op: bool = False, **kwargs):
        if return_op:
            return pymongo.UpdateOne({ '_id': unique_id }, { '$set': kwargs })

        await self.collection.update_one({ '_id': unique_id }, { '$set': kwargs })

    async def update_live_stats(self, box_scores: list[dict]) -> None:
        requests = []
        for box_score in box_scores:
            subject_filtered_betting_lines = await self.get_betting_lines({
                'subject': box_score['subject']['name'] , 'game._id': box_score['game']['_id']
            })
            for betting_line in subject_filtered_betting_lines:
                market = betting_line['market']
                if stat := box_score['box_score'].get(market):
                    update_op = await self.update_betting_line(betting_line['_id'], return_op=True, live_stat=stat)
                    requests.append(update_op)

        if requests:
            await self.collection.bulk_write(requests)

    @staticmethod
    def _refactor_live_to_final_stat(betting_lines: list[dict]):
        for betting_line in betting_lines:
            final_stat = betting_line.pop('live_stat', None)
            betting_line['final_stat'] = final_stat

    @staticmethod
    def _prepare_betting_lines_for_upload(betting_lines: list[dict]) -> str:
        BettingLines._refactor_live_to_final_stat(betting_lines)
        betting_lines_json = json.dumps(betting_lines)
        return betting_lines_json

    @staticmethod
    def _upload_to_gcs(betting_lines_json: str) -> None:
        storage_client = storage.Client()
        bucket = storage_client.bucket("betting-lines")  # Todo: set to env variable
        blob = bucket.blob(f"{datetime.now().strftime('%Y-%m-%d')}.json")
        blob.upload_from_string(betting_lines_json)

    async def update_betting_line_results(self, game_ids: list[dict]) -> None:
        # Todo: ANY FINAL STATS TO ADD?
        betting_lines_filtered_by_game = await self.get_betting_lines({ 'game._id': { '$in': game_ids } })
        betting_lines_json = self._prepare_betting_lines_for_upload(betting_lines_filtered_by_game)
        self._upload_to_gcs(betting_lines_json)

    async def delete_betting_lines(self):
        await self.collection.delete_many({})
