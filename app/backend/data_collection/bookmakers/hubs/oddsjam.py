from datetime import datetime
import asyncio
from typing import Optional
import math

from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers import utils as bkm_utils



class OddsJam(bkm_utils.LinesRetriever):
    def __init__(self, lines_hub: bkm_utils.LinesSource):
        # call parent class Plug
        super().__init__(lines_hub)