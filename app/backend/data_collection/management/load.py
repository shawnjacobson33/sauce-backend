import asyncio
import time
from asyncio import Task

from app.backend.data_collection import workers as wrk

from app.backend.data_collection.management.utils import report_line_counts
from app.backend.data_collection.management.configure import configure_game_retriever, configure_lines_retriever


def cleanup(retriever: wrk.Retriever):
    # create the decorator function
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # track the time it takes to execute the function
            t1 = time.time()
            result = await func(*args, **kwargs)
            t2 = time.time()
            # output the statistics from the job
            report_line_counts(retriever, t2-t1)
            # return the function call as part of the decorator
            return result

        return wrapper

    return decorator


async def load_retriever(retriever: wrk.Retriever):
    # a decorator to log metrics and save samples of the data in a file
    @cleanup(retriever)
    async def retrieve():
        # wait until the plug is finished collecting its data
        await retriever.retrieve()

    # wait until the collection process is done
    return retrieve


async def load_roster_tasks(roster_retriever_names: list[str] = None):
    # 1. First Get Schedule Classes
    roster_retriever_classes = wrk.ROSTER_RETRIEVERS.items() if not roster_retriever_names else [
        (roster_retriever_name, wrk.ROSTER_RETRIEVERS[roster_retriever_name]) for roster_retriever_name in
        roster_retriever_names]

    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for source_name, roster_retriever_class in roster_retriever_classes:
        # configure a bookmaker plug to collect data
        roster_retriever = configure_game_retriever(source_name, roster_retriever_class)
        # get the retrieve coroutine
        coro = await load_retriever(roster_retriever)
        # start collecting
        tasks.append(asyncio.create_task(coro))

    return tasks


async def load_box_score_tasks(box_score_retriever_names: list[str] = None):
    # 1. First Get Schedule Classes
    box_score_retriever_classes = wrk.BOX_SCORE_RETRIEVERS.items() if not box_score_retriever_names else [
        (box_score_retriever_name, wrk.BOX_SCORE_RETRIEVERS[box_score_retriever_name]) for box_score_retriever_name in
        box_score_retriever_names]

    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for source_name, box_score_retriever_class in box_score_retriever_classes:
        # configure a bookmaker plug to collect data
        box_score_retriever = configure_game_retriever(source_name, box_score_retriever_class)
        # get the retrieve coroutine
        coro = await load_retriever(box_score_retriever)
        # start collecting
        tasks.append(asyncio.create_task(coro))

    return tasks


async def load_schedule_tasks(worker_names: list[str] = None) -> list[Task]:
    # 1. First Get Schedule Classes
    schedules_retriever_classes = wrk.SCHEDULE_RETRIEVERS.items() if not worker_names else [
        (schedule_retriever_name, wrk.SCHEDULE_RETRIEVERS[schedule_retriever_name]) for schedule_retriever_name in
        worker_names]
    
    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for source_name, schedule_retriever_class in schedules_retriever_classes:
        # configure a bookmaker plug to collect data
        schedule_retriever = configure_game_retriever(source_name, schedule_retriever_class)
        # get the retrieve coroutine
        coro = await load_retriever(schedule_retriever)
        # start collecting
        tasks.append(asyncio.create_task(coro))

    return tasks


async def load_line_tasks(group: str, worker_names: list[str] = None) -> list[Task]:
    # get all the bookmaker plugs to run
    line_retriever_classes = wrk.LINE_WORKERS[group].values() if not worker_names else [
        wrk.LINE_WORKERS[group][worker] for worker in worker_names]

    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for line_retriever_class in line_retriever_classes:
        # configure a bookmaker plug to collect data
        line_retriever = configure_lines_retriever(line_retriever_class)
        # get the retrieve coroutine
        coro = await load_retriever(line_retriever)
        # start collecting
        tasks.append(asyncio.create_task(coro))

    return tasks
