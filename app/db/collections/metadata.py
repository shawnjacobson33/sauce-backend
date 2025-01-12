from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db.base import BaseCollection


class Metadata(BaseCollection):

    def __init__(self, db: AsyncIOMotorDatabase):
        super().__init__(db)
        self.collection = self.db['metadata']

    async def get_ev_formula(self, market_domain: str, ev_formula_name: str) -> str | None:
        metadata = await self.collection.find_one({'_id': 'metadata'})
        if market_domain_dict := metadata['ev_algo'].get(market_domain):
            if ev_formula := market_domain_dict.get(ev_formula_name):
                ev_formula['name'] = ev_formula_name
                return ev_formula

            raise ValueError(f"EV formula '{ev_formula_name}' not found in '{market_domain}' market domain")

        raise ValueError(f"Market domain '{market_domain}' not found in metadata")