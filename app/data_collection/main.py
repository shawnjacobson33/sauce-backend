import os
import time
import asyncio

from app.data_collection import utils as dc_utils
from app.data_collection.bookmakers import BettingLines


async def run_schedules_retrieving_tasks(schedule_retriever_names: list[str] = None) -> float:
    # get the coroutines
    schedule_retrieving_tasks = dc_utils.launch_schedules_retrievers(schedule_retriever_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*schedule_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    return round(t2-t1, 3)


async def run_lines_retrieving_tasks(lines_retriever_names: list[str] = None) -> float:
    # get the coroutines
    lines_retrieving_tasks = dc_utils.launch_lines_retrievers(lines_retriever_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*lines_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    return round(t2-t1, 3)


async def retrieve_and_report(schedule_retriever_names: list[str] = None, lines_retriever_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Schedule Retrieval {"*" * 22}\n')
    # run the schedule retrieving tasks first
    schedules_retrieving_time = await run_schedules_retrieving_tasks(schedule_retriever_names)
    # TODO: Insert Game Ids into Database
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL GAMES]: {dc_utils.Games.size()}, {schedules_retrieving_time}s\n")
    # section header
    print(f'{"*" * 22} Lines Retrieval {"*" * 22}\n')
    # run the lines retrieving tasks second
    lines_retrieving_time = await run_lines_retrieving_tasks(lines_retriever_names)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL LINES]: {BettingLines.size()}, {lines_retrieving_time}s")
    # save all output data to json files
    dc_utils.save_data_to_files()
    # output the size of the file storing the betting lines
    print(f"[FILE SIZE]: {round(os.path.getsize('utils/reports/betting_lines.json') / (1024 ** 2), 2)} MB")


def execute():
    asyncio.run(retrieve_and_report())


if __name__ == '__main__':
    execute()
