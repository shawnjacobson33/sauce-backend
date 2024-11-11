import os
import time
import asyncio

from app.data_collection import games as gms
from app.data_collection import bookmakers as bkm
from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import BettingLines
from app.data_collection.utils import configure_lines_retriever


async def run(schedule_collector: str, plug: str, run_all: bool = False) -> None:


    # get all the bookmaker plugs to run
    lines_retriever_classes = LINES_RETRIEVERS.values() if run_all else [LINES_RETRIEVERS[plug]]
    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for line_retriever_class in lines_retriever_classes:
        # configure a bookmaker plug to collect data
        line_retriever = dc_utils.configure_lines_retriever(line_retriever_class)
        # start collecting
        tasks.append(launch_retriever(line_retriever))

    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*tasks)
    t2 = time.time()
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL]: {BettingLines.size()}, {round(t2-t1, 3)}s")
    # save all output data to json files
    dc_utils.save_data_to_files()
    # output the size of the file storing the betting lines
    print(f"[FILE SIZE]: {round(os.path.getsize('utils/reports/betting_lines.json') / (1024 ** 2), 2)} MB")


if __name__ == '__main__':
    asyncio.run(run("", "", run_all=True))
