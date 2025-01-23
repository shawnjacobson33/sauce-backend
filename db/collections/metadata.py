from motor.motor_asyncio import AsyncIOMotorDatabase

from db.base_collection import BaseCollection


class Metadata(BaseCollection):
    """
    A class to manage metadata in the database.

    Attributes:
        db (AsyncIOMotorDatabase): The database connection.
        collection (AsyncIOMotorDatabase.collection): The metadata collection.
    """

    def __init__(self, db: AsyncIOMotorDatabase):
        """
        Initializes the Metadata class with the given database connection.

        Args:
            db (AsyncIOMotorDatabase): The database connection.
        """
        super().__init__('metadata', db)

    async def get_ev_formula(self, market_domain: str, ev_formula_name: str) -> str | None:
        """
        Retrieves the EV formula for the given market domain and formula name.

        Args:
            market_domain (str): The market domain to search in.
            ev_formula_name (str): The name of the EV formula to retrieve.

        Returns:
            str | None: The EV formula if found, otherwise None.

        Raises:
            ValueError: If the market domain or EV formula is not found.
        """
        metadata = await self.collection.find_one({'_id': 'metadata'})
        if market_domain_dict := metadata['ev_formulas'].get(market_domain):
            if ev_formula := market_domain_dict.get(ev_formula_name):
                ev_formula['name'] = ev_formula_name
                return ev_formula

            raise ValueError(f"EV formula '{ev_formula_name}' not found in '{market_domain}' market domain")

        raise ValueError(f"Market domain '{market_domain}' not found in metadata")