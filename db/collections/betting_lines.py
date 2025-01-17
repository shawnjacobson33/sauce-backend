import json
from collections import defaultdict
from datetime import datetime

import pymongo
from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base import BaseCollection
from db.collections.utils import GCSUploader


class BettingLines(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)

        self.collection = self.db['betting_lines']
        self.completed_betting_lines_collection = self.db['completed_betting_lines']

        self.gcs_uploader = GCSUploader(bucket_name='betting-lines')

    async def get_betting_line(self, query: dict) -> dict:
        return await self.collection.find_one(query)

    @staticmethod
    def _get_most_recent_stream_record(stream: list[dict]):
        most_recent_stream_record = stream[-1]
        for k, v in most_recent_stream_record.items():
            if isinstance(v, list):
                most_recent_stream_record[k] = v[-1]

        return most_recent_stream_record

    async def _get_most_recent_betting_lines(self, query: dict, proj: dict) -> list[dict]:
        most_recent_betting_lines = []
        async for betting_line in self.collection.find(query, {'stream': { '$slice': -1 }, **proj}):
            most_recent_stream_record = self._get_most_recent_stream_record(betting_line['stream'])
            most_recent_betting_lines.append(
                { **{k: v for k, v in betting_line.items() if k != 'stream'}, **most_recent_stream_record }
            )

        return most_recent_betting_lines

    async def get_betting_lines(self, query: dict, proj: dict = None, most_recent: bool = False) -> list[dict]:
        if most_recent:
            return await self._get_most_recent_betting_lines(query, proj if proj else {})

        return await self.collection.find(query, proj if proj else {}).to_list()

    async def get_completed_betting_lines(self, query: dict, proj: dict = None):
        return await self.completed_betting_lines_collection.find(query, proj if proj else {}).to_list()

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
    def _restructure_betting_lines_data(betting_lines: list[dict]):
        for betting_line in betting_lines:
            final_stat = betting_line.pop('live_stat', None)
            betting_line['final_stat'] = final_stat

            game = betting_line.pop('game')
            betting_line['game_id'] = game['_id']

    @staticmethod
    def _prepare_betting_lines_for_upload(betting_lines: list[dict]) -> str:
        BettingLines._restructure_betting_lines_data(betting_lines)
        betting_lines_json = json.dumps(betting_lines)
        return betting_lines_json

    async def _store_in_gcs(self):
        completed_betting_lines = await self.get_completed_betting_lines({})
        betting_lines_json = self._prepare_betting_lines_for_upload(completed_betting_lines)
        blob_name = f"{datetime.now().strftime('%Y-%m-%d')}.json"
        self.gcs_uploader.upload(blob_name, betting_lines_json)

    @staticmethod
    def _get_projection_for_gcs() -> dict:
        return {
            'one_click_url': 0, 'metrics': 0, 'subject': 0, 'bookmaker': 0, 'date': 0, 'label': 0,
            'league': 0, 'market': 0
        }

    async def store_completed_betting_lines(self, game_ids: list[dict] = None, in_gcs: bool = False) -> None:
        # Todo: ANY FINAL STATS TO ADD?, OUTPUT THE SIZE OF THE JSON FILE
        if game_ids:
            optimized_projection = self._get_projection_for_gcs()
            betting_lines_filtered_by_game = await self.get_betting_lines({
                'game._id': { '$in': game_ids },
            }, optimized_projection)
            betting_lines_filtered_for_live_stat = [ # only want to store betting lines that have a final box score stat
                betting_line for betting_line in betting_lines_filtered_by_game if betting_line.get('live_stat')
            ]
            self.completed_betting_lines_collection.insert_many(betting_lines_filtered_for_live_stat)
            return await self.delete_betting_lines(betting_lines_filtered_by_game)

        if in_gcs:
            await self._store_in_gcs()
            return await self.completed_betting_lines_collection.delete_many({})

    async def delete_betting_lines(self, betting_lines: list[dict] = None) -> None:
        if betting_lines:
            betting_line_ids = [betting_line['_id'] for betting_line in betting_lines]
            await self.collection.delete_many({ '_id': { '$in': betting_line_ids } })

        await self.collection.delete_many({})
