import os
import time
import asyncio

from app.backend.database import db
from app.backend.data_collection import utils as dc_utils
from app.backend.data_collection.bookmakers import BettingLines


async def run_roster_retrieving_tasks(logistic_retriever_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Roster Retrieval {"*" * 22}\n')
    # TODO: Realistically these retrievers should only run once a day
    # get the coroutines
    roster_retrieving_tasks = dc_utils.launch_roster_retrievers(logistic_retriever_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*roster_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    roster_retrieving_time = round(t2 - t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL SUBJECTS]: {dc_utils.Subjects.size()}, {roster_retrieving_time}s\n")
    

async def run_box_score_retrieving_tasks(logistic_retriever_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Box Score Retrieval {"*" * 22}\n')
    # initialize a time of zero
    box_score_retrieving_time = 0
    # This will run logic to delete any games that finished (no longer need game ids after they are done)
    if started_games := db.MongoDB.fetch_started_games():
        # store any currently running games in the data structure
        dc_utils.ActiveGames.update_games(started_games)
        # get the coroutines
        box_score_retrieving_tasks = dc_utils.launch_box_score_retrievers(logistic_retriever_names)
        # start making requests asynchronously
        t1 = time.time()
        await asyncio.gather(*box_score_retrieving_tasks)
        t2 = time.time()
        # return time taken to complete retrieving tasks
        box_score_retrieving_time = round(t2 - t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[ACTIVE GAMES]: {dc_utils.BoxScores.size()}, {box_score_retrieving_time}s\n")


async def run_schedules_retrieving_tasks(schedule_retriever_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Schedule Retrieval {"*" * 22}\n')
    # get the coroutines
    schedule_retrieving_tasks = dc_utils.launch_schedules_retrievers(schedule_retriever_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*schedule_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    schedules_retrieving_time = round(t2-t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[SCHEDULED GAMES]: {dc_utils.Games.size()}, {schedules_retrieving_time}s\n")


async def run_lines_retrieving_tasks(lines_retriever_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Lines Retrieval {"*" * 22}\n')
    # get the coroutines
    lines_retrieving_tasks = dc_utils.launch_lines_retrievers(lines_retriever_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*lines_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    lines_retrieving_time = round(t2-t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL LINES]: {BettingLines.size()}, {lines_retrieving_time}s")


async def retrieve_and_report(logistic_retriever_names: list[str] = None, lines_retriever_names: list[str] = None) -> None:
    # # run the roster retrieving tasks
    # await run_roster_retrieving_tasks(logistic_retriever_names)
    # run the box score retrieving tasks
    await run_box_score_retrieving_tasks(logistic_retriever_names)
    # run the schedule retrieving tasks
    await run_schedules_retrieving_tasks(logistic_retriever_names)
    # # run the lines retrieving tasks second
    # await run_lines_retrieving_tasks(lines_retriever_names)
    # save all output data to json files
    dc_utils.save_data_to_files()
    # output the size of the file storing the betting lines
    print(f"[FILE SIZE]: {round(os.path.getsize('utils/reports/betting_lines.json') / (1024 ** 2), 2)} MB")


def execute(logistic_retriever_names: list[str] = None, lines_retriever_names: list[str] = None):
    asyncio.run(retrieve_and_report(logistic_retriever_names, lines_retriever_names))


if __name__ == '__main__':
    execute()
