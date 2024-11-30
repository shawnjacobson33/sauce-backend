import asyncio
import time

import app.backend.data_collection.management.load as ld
from app.backend.data_collection.workers.utils import BettingLines, Games, BoxScores, Subjects


async def execute_roster_tasks(worker_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Roster Retrieval {"*" * 22}\n')
    # get the coroutines
    roster_retrieving_tasks = await ld.load_roster_tasks(worker_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*roster_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    roster_retrieving_time = round(t2 - t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL SUBJECTS]: {Subjects.counts()}, {roster_retrieving_time}s\n")


async def execute_schedule_tasks(worker_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Schedule Retrieval {"*" * 22}\n')
    # get the coroutines
    schedule_tasks = await ld.load_schedule_tasks(worker_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*schedule_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    retrieval_time = round(t2 - t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL SCHEDULED GAMES]: {Games.counts()}, {retrieval_time}s\n")


async def execute_box_score_tasks(worker_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Box Score Retrieval {"*" * 22}\n')
    # get the coroutines
    box_score_retrieving_tasks = await ld.load_box_score_tasks(worker_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*box_score_retrieving_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    box_score_retrieving_time = round(t2 - t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL ACTIVE GAMES]: {BoxScores.counts()}, {box_score_retrieving_time}s\n")


async def execute_line_tasks(group: str, worker_names: list[str] = None) -> None:
    # section header
    print(f'{"*" * 22} Lines Retrieval {"*" * 22}\n')
    # get the coroutines
    line_tasks = await ld.load_line_tasks(group, worker_names)
    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*line_tasks)
    t2 = time.time()
    # return time taken to complete retrieving tasks
    retrieval_time = round(t2-t1, 3)
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL LINES]: {BettingLines.counts()}, {retrieval_time}s")
