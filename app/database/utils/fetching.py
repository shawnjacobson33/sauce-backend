from typing import Optional

from app.database.db import MongoDB
from app.database.utils.definitions import SOURCES_COLLECTION_NAME


def fetch_source(source_name: str) -> Optional[dict]:
    if source := MongoDB.fetch_collection(SOURCES_COLLECTION_NAME).find_one({'name': source_name}):
        return source

