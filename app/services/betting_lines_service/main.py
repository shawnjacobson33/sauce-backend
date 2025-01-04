import asyncio
import time

from app.services.betting_lines_service.data_collection import run_oddsshopper_scraper
from app.services.betting_lines_service.data_evaluation import run_ev_calculator


async def run_prop_lines_pipeline():
    while True:
        start_time = time.time()
        print('Running prop lines pipeline...')
        print('Starting data collection...')
        betting_lines = await run_oddsshopper_scraper()
        print('Finished data collection...')
        # Todo: should be multi-processed
        print('Starting data evaluation...')
        evaluated_betting_lines = await run_ev_calculator(betting_lines)
        print('Finished data evaluation...')
        # Todo: should be async
        print('Saving evaluated betting lines...')
        evaluated_betting_lines.to_csv('~/PycharmProjects/sauce-api/app/services/betting_lines_service/evaluated_betting_lines.csv', index=False)
        end_time = time.time()
        print(f'Pipeline completed in {round(end_time - start_time, 2)} seconds. Sleeping for 60 seconds...')
        await asyncio.sleep(60)


if __name__ == '__main__':
    asyncio.run(run_prop_lines_pipeline())
