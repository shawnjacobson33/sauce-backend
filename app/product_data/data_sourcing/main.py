import asyncio
import sys
import time
import uuid

from app.product_data.data_sourcing.shared_data import PropLines
from app.product_data.data_sourcing.utils import RequestManager, DataStandardizer, Bookmaker, get_bookmaker, get_db
from app.product_data.data_sourcing.plugs import BetOnline, BoomFantasy, Champ, Dabble, Drafters, DraftKingsPick6, \
    HotStreak, MoneyLine, OwnersBox, ParlayPlay, Payday, PrizePicks, Rebet, Sleeper, SuperDraft, UnderdogFantasy, \
    VividPicks, OddsShopper


class DataCollector:
    def __init__(self):
        self.batch_id = str(uuid.uuid4())
        self.db = get_db()

    async def collect(self):
        plugs = [BetOnline, BoomFantasy, Dabble, Drafters, DraftKingsPick6, HotStreak, MoneyLine, OwnersBox, ParlayPlay,
                 Payday, PrizePicks, Rebet, Sleeper, SuperDraft, UnderdogFantasy, VividPicks, OddsShopper]

        tasks = []
        for plug in plugs:
            info = Bookmaker(get_bookmaker(self.db, plug.__name__))
            request_manager = RequestManager(use_requests=(plug.__name__ == 'BetOnline'))
            data_standardizer = DataStandardizer(self.batch_id, self.db, has_grouping=(plug.__name__ != 'Drafters'))
            tasks.append(plug(info, self.batch_id, request_manager, data_standardizer).start())

        await asyncio.gather(*tasks)
        print(f'Data Collection Complete: {self.batch_id}')


async def main():
    start_time = time.time()
    with open('logs/standardizing_log.txt', 'w') as f:
        sys.stdout = f
        await DataCollector().collect()

    sys.stdout = sys.__stdout__
    end_time = time.time()

    print(f'[ALL]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
