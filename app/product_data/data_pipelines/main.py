import asyncio
import time
import uuid

from app.product_data.data_pipelines.request_management import AsyncRequestManager

from app.product_data.data_pipelines.primary_sources import BoomFantasySpider, ChampSpider, DabbleSpider, \
    DraftersSpider, \
    DraftKingsPick6, HotStreakSpider, MoneyLineSpider, OwnersBoxSpider, ParlayPlaySpider, PaydaySpider, \
    PrizePicksSpider, \
    RebetSpider, SleeperSpider, SuperDraftSpider, ThriveFantasySpider, UnderdogSpider, VividPicksSpider
from app.product_data.data_pipelines.secondary_sources import OddsShopperSpider, SmartBettorSpider


class DataPipeline:
    def __init__(self):
        self.batch_id = uuid.uuid4()
        self.arm = AsyncRequestManager()

    async def collect(self):
        spiders = [BoomFantasySpider, ChampSpider, DabbleSpider, DraftersSpider, DraftKingsPick6,
                   HotStreakSpider, MoneyLineSpider, OwnersBoxSpider, ParlayPlaySpider, PaydaySpider,
                   PrizePicksSpider, RebetSpider, SleeperSpider, SuperDraftSpider, ThriveFantasySpider,
                   UnderdogSpider, VividPicksSpider, OddsShopperSpider, SmartBettorSpider]

        tasks = []
        for spider in spiders:
            tasks.append(spider(self.batch_id, self.arm).start())

        await asyncio.gather(*tasks)
        print(f'Data Collection Complete: {self.batch_id}')


async def main():
    start_time = time.time()
    await DataPipeline().collect()
    end_time = time.time()

    print(f'[ALL]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
