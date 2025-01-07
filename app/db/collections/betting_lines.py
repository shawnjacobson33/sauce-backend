from pymongo import InsertOne, UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient


class BettingLines:
    def __init__(self, client: AsyncIOMotorClient):
        self.client = client
        self.db = client['sauce-dev']
        self.collection = self.db['betting_lines']

    async def get_betting_line(self, query: dict) -> dict:
        return await self.collection.find_one(query, { '_id': 0 })

    async def get_betting_lines(self, query: dict) -> list[dict]:
        return await self.collection.find(query, { '_id': 0 }).to_list()

    @staticmethod
    def _create_doc(line: dict):
        return {
            **{k: line[k] for k in ['_id', 'bookmaker', 'league', 'subject', 'market', 'label']},
            'records': [BettingLines._create_record(line)]
        }

    @staticmethod
    def _create_record(line: dict) -> dict:
        return {
            'line': line['line'],
            'odds': line['odds'],
            'impl_prb': 1 / line['odds'],
            'timestamp': [line['timestamp']]
        }

    async def store_betting_lines(self, betting_lines: list[dict]) -> None:
        requests = []
        for betting_line_dict in betting_lines:
            if not (betting_line_doc_match := await self.get_betting_line(betting_line_dict['_id'])):
                records = betting_line_doc_match['records']
                most_recent_record = records[-1]
                if (not (betting_line_dict['line'] == most_recent_record['line']) or
                    not (betting_line_dict['odds'] == most_recent_record['odds'])):

                    new_record = self._create_record(betting_line_dict)
                    records.append(new_record)

                most_recent_record['timestamp'].append(betting_line_dict['timestamp'])
                update_op = self.update_betting_line(betting_line_dict['_id'], return_op=True, records=records)  # Todo: do you need to replace the entire records field?
                requests.append(update_op)

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
