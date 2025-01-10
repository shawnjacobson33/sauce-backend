from pymongo import InsertOne, UpdateOne
from motor.motor_asyncio import AsyncIOMotorDatabase

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

    async def get_betting_lines(self, query: dict, most_recent: bool = True) -> list[dict]:
        if most_recent:
            return await self._get_most_recent_betting_lines(query)

        return await self.collection.find(query).to_list()

    @staticmethod
    def _create_doc(line: dict):
        return {
            **{k: line[k] for k in ['_id', 'bookmaker', 'league', 'subject', 'market', 'label']},
            'stream': [ BettingLines._create_record(line) ]
        }

    @staticmethod
    def _create_record(line: dict) -> dict:
        return {
            'batch_num': [line['batch_num']],
            'line': line['line'],
            'odds': line['odds'],
            'impl_prb': line['impl_prb'],
            'tw_prb': line.get('tw_prb'),
            'ev': line.get('ev'),
            'batch_timestamp': [line['batch_timestamp']],
            'collection_timestamp': [line['collection_timestamp']]
        }

    async def store_betting_lines(self, betting_lines: list[dict]) -> None:
        requests = []
        for betting_line_dict in betting_lines:
            if betting_line_doc_match := await self.get_betting_line(betting_line_dict['_id']):
                stream = betting_line_doc_match['stream']
                most_recent_record = stream[-1]
                if (not (betting_line_dict['line'] == most_recent_record['line']) or
                    not (betting_line_dict['odds'] == most_recent_record['odds']) or
                    not (betting_line_dict['ev'] == most_recent_record['ev'])):

                    new_record = self._create_record(betting_line_dict)
                    stream.append(new_record)

                else:
                    most_recent_record['batch_num'].append(betting_line_dict['batch_num'])
                    most_recent_record['batch_timestamp'].append(betting_line_dict['batch_timestamp'])
                    most_recent_record['collection_timestamp'].append(betting_line_dict['collection_timestamp'])

                update_op = await self.update_betting_line(betting_line_dict['_id'], return_op=True,
                                                           stream=stream)  # Todo: do you need to replace the entire records field?
                requests.append(update_op)

            else:
                new_betting_line_doc = self._create_doc(betting_line_dict)
                insert_op = InsertOne(new_betting_line_doc)
                requests.append(insert_op)

        await self.collection.bulk_write(requests)

    async def update_betting_line(self, unique_id: str, return_op: bool = False, **kwargs):
        if return_op:
            return UpdateOne({ '_id': unique_id }, { '$set': kwargs })

        await self.collection.update_one({ '_id': unique_id }, { '$set': kwargs })

    async def delete_betting_lines(self):
        await self.collection.delete_many({})
