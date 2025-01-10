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
    def _get_most_recent_betting_lines_projection():
        return {
            'stream.line': { '$slice': -1 },
            'stream.odds': { '$slice': -1 },
            'stream.impl_prb': { '$slice': -1 },
            'stream.tw_prb': { '$slice': -1 },
            'stream.ev': { '$slice': -1 },
            'stream.batch_timestamp': { '$slice': -1 },
            'stream.collection_timestamp': { '$slice': -1 }
        }

    async def get_most_recent_betting_lines(self, query: dict) -> list[dict]:
        most_recent_betting_lines = []
        projection = self._get_most_recent_betting_lines_projection()
        async for betting_line in self.collection.find(query, projection):
            stream = {k: v[0][k] for k, v in betting_line['stream'].items()}
            most_recent_betting_lines.append({ **{k: v for k, v in betting_line.items() if k != 'stream'}, **stream })

        return most_recent_betting_lines

    async def get_betting_lines(self, query: dict) -> list[dict]:
        return await self.collection.find(query).to_list()

    @staticmethod
    def _create_doc(line: dict):
        return {
            **{k: line[k] for k in ['_id', 'bookmaker', 'league', 'subject', 'market', 'label']},
            'stream': BettingLines._create_stream(line)
        }

    @staticmethod
    def _create_stream(line: dict) -> dict:
        # Todo: are batch nums actually necessary?
        return {
            'line': [{ 'batch_num': 0, 'line': line['line'] }],
            'odds': [{ 'batch_num': 0, 'odds': line['odds'] }],
            'impl_prb': [{ 'batch_num': 0, 'impl_prb': line['impl_prb'] }],
            'tw_prb': [{ 'batch_num': 0, 'tw_prb': line['tw_prb'] }],
            'ev': [{ 'batch_num': 0, 'ev': line['ev'] }],
            'batch_timestamp': [ { 'batch_num': 0, 'batch_timestamp': line['batch_timestamp'] } ],
            'collection_timestamp': [ { 'batch_num': 0, 'collection_timestamp': line['collection_timestamp'] } ],
        }

    async def store_betting_lines(self, betting_lines: list[dict]) -> None:
        requests = []
        for betting_line_dict in betting_lines:
            if betting_line_doc_match := await self.get_betting_line(betting_line_dict['_id']):

                stream = betting_line_doc_match['stream']
                stream_lines, stream_odds, stream_ev = stream['line'], stream['odds'], stream['ev']

                if not (betting_line_dict['line'] == stream_lines[-1]['line']):
                    stream_lines.append({ 'batch_num': betting_line_dict['batch_num'], 'line': betting_line_dict['line'] })
                if not (betting_line_dict['odds'] == stream_odds[-1]['odds']):
                    stream_odds.append({ 'batch_num': betting_line_dict['batch_num'], 'odds': betting_line_dict['odds'] })
                    stream['impl_prb'].append({ 'batch_num': betting_line_dict['batch_num'], 'impl_prb': betting_line_dict['impl_prb'] })
                if not (betting_line_dict['ev'] == stream_ev[-1]['ev']):
                    stream_ev.append({ 'batch_num': betting_line_dict['batch_num'], 'ev': betting_line_dict['ev'] })
                    stream['tw_prb'].append({ 'batch_num': betting_line_dict['batch_num'], 'tw_prb': betting_line_dict['tw_prb'] })

                stream['batch_timestamp'].append({ 'batch_num': betting_line_dict['batch_num'], 'batch_timestamp': betting_line_dict['batch_timestamp'] })
                stream['collection_timestamp'].append({ 'batch_num': betting_line_dict['batch_num'], 'collection_timestamp': betting_line_dict['collection_timestamp'] })

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
