import asyncio
import time
import uuid
import threading

from app.product_data.data_sourcing.utils import RequestManager, DataStandardizer, Bookmaker, get_bookmaker, get_db

from app.product_data.data_sourcing.plugs import BetOnline, BoomFantasy, Champ, Dabble, Drafters, DraftKingsPick6, HotStreak, \
    MoneyLine, OwnersBox, ParlayPlay, Payday, PrizePicks, Rebet, Sleeper, SuperDraft, UnderdogFantasy, VividPicks, \
    OddsShopper


class DataCollector:
    def __init__(self):
        self.batch_id = str(uuid.uuid4())
        self.db = get_db()
        self.lock = threading.Lock()

    async def collect(self):
        plugs = [BetOnline, BoomFantasy, Champ, Dabble, Drafters, DraftKingsPick6, HotStreak, MoneyLine, OwnersBox, ParlayPlay,
                 Payday, PrizePicks, Rebet, Sleeper, SuperDraft, UnderdogFantasy, VividPicks, OddsShopper]

        tasks = []
        for plug in plugs:
            info = Bookmaker(get_bookmaker(self.db, plug.__name__))
            tasks.append(plug(info, self.batch_id, RequestManager(), DataStandardizer(self.batch_id, self.db)).start())

        await asyncio.gather(*tasks)
        print(f'Data Collection Complete: {self.batch_id}')


async def main():
    start_time = time.time()
    await DataCollector().collect()
    end_time = time.time()

    print(f'[ALL]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
