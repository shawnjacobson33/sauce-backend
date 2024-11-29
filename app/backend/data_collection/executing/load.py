import asyncio
import time
from asyncio import Task
from multiprocessing.pool import worker

from app.backend.data_collection.bookmakers import LINE_RETRIEVERS
from app.backend.data_collection.logistics import BOX_SCORE_RETRIEVERS, SCHEDULE_RETRIEVERS, ROSTER_RETRIEVERS

from app.backend.data_collection.utils.modelling.base_models import Retriever
from app.backend.data_collection.executing.output import output_source_stats
from app.backend.data_collection.executing.configure import configure_game_retriever, configure_lines_retriever


def cleanup(retriever: Retriever):
    # create the decorator function
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # track the time it takes to execute the function
            t1 = time.time()
            result = await func(*args, **kwargs)
            t2 = time.time()
            # output the statistics from the job
            output_source_stats(retriever, t2-t1)
            # return the function call as part of the decorator
            return result

        return wrapper

    return decorator


async def launch_retriever(retriever: Retriever):
    # a decorator to log metrics and save samples of the data in a file
    @cleanup(retriever)
    async def retrieve():
        # wait until the plug is finished collecting its data
        await retriever.retrieve()

    # wait until the collection process is done
    await retrieve()


def launch_roster_retrievers(roster_retriever_names: list[str] = None):
    # 1. First Get Schedule Classes
    roster_retriever_classes = ROSTER_RETRIEVERS.items() if not roster_retriever_names else [
        (roster_retriever_name, ROSTER_RETRIEVERS[roster_retriever_name]) for roster_retriever_name in
        roster_retriever_names]

    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for source_name, roster_retriever_class in roster_retriever_classes:
        # configure a bookmaker plug to collect data
        roster_retriever = configure_game_retriever(source_name, roster_retriever_class)
        # get the retrieve task
        task = launch_retriever(roster_retriever)
        # start collecting
        tasks.append(asyncio.create_task(task))

    return tasks


def launch_box_score_retrievers(box_score_retriever_names: list[str] = None):
    # 1. First Get Schedule Classes
    box_score_retriever_classes = BOX_SCORE_RETRIEVERS.items() if not box_score_retriever_names else [
        (box_score_retriever_name, BOX_SCORE_RETRIEVERS[box_score_retriever_name]) for box_score_retriever_name in
        box_score_retriever_names]

    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for source_name, box_score_retriever_class in box_score_retriever_classes:
        # configure a bookmaker plug to collect data
        box_score_retriever = configure_game_retriever(source_name, box_score_retriever_class)
        # start collecting
        tasks.append(asyncio.create_task(launch_retriever(box_score_retriever)))

    return tasks


def launch_schedule_retrievers(schedule_retriever_names: list[str] = None):
    # 1. First Get Schedule Classes
    schedules_retriever_classes = SCHEDULE_RETRIEVERS.items() if not schedule_retriever_names else [
        (schedule_retriever_name, SCHEDULE_RETRIEVERS[schedule_retriever_name]) for schedule_retriever_name in
        schedule_retriever_names]
    
    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for source_name, schedule_retriever_class in schedules_retriever_classes:
        # configure a bookmaker plug to collect data
        schedule_retriever = configure_game_retriever(source_name, schedule_retriever_class)
        # start collecting
        tasks.append(asyncio.create_task(launch_retriever(schedule_retriever)))

    return tasks


def load_line_tasks(group: str, worker_names: list[str] = None) -> list[Task]:
    # get all the bookmaker plugs to run
    line_retriever_classes = LINE_RETRIEVERS[group].values() if not worker_names else [
        LINE_RETRIEVERS[group][worker] for worker in worker_names]

    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for line_retriever_class in line_retriever_classes:
        # configure a bookmaker plug to collect data
        line_retriever = configure_lines_retriever(line_retriever_class)
        # start collecting
        tasks.append(asyncio.create_task(launch_retriever(line_retriever)))

    return tasks
