import time
from datetime import datetime

from backend.app.data_collection import workers as wrk

from backend.app.data_collection.management.configure import configure_game_retriever, configure_lines_retriever


BATCH_DAY = datetime.now().day
BATCH_NUM = 0


def report(retriever: wrk.Retriever):
    # create the decorator function
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # track the time it takes to execute the function
            t1 = time.time()
            result = await func(*args, **kwargs)
            t2 = time.time()
            # output the statistics from the job
            # repor(retriever, t2-t1)
            # return the function call as part of the decorator
            return result

        return wrapper

    return decorator


async def load_retriever(retriever: wrk.Retriever):
    # a decorator to log metrics and save samples of the data in a file
    @report(retriever)
    async def retrieve():
        # wait until the plug is finished collecting its data
        await retriever.retrieve()

    # wait until the collection process is done
    await retrieve()


async def load_roster_tasks(roster_retriever_names: list[str] = None):
    # 1. First Get Schedule Classes
    roster_retriever_classes = wrk.ROSTER_RETRIEVERS.items() if not roster_retriever_names else [
        (roster_retriever_name, wrk.ROSTER_RETRIEVERS[roster_retriever_name]) for roster_retriever_name in
        roster_retriever_names]

    coros = [
        load_retriever(configure_game_retriever(source_name, retriever_class))
        for source_name, retriever_class in roster_retriever_classes
    ]

    return coros


async def load_box_score_tasks(box_score_retriever_names: list[str] = None) -> list:
    # 1. First Get Schedule Classes
    box_score_retriever_classes = wrk.BOX_SCORE_RETRIEVERS.items() if not box_score_retriever_names else [
        (box_score_retriever_name, wrk.BOX_SCORE_RETRIEVERS[box_score_retriever_name]) for box_score_retriever_name in
        box_score_retriever_names]

    coros = [
        load_retriever(configure_game_retriever(source_name, retriever_class))
        for source_name, retriever_class in box_score_retriever_classes
    ]

    return coros


async def load_schedule_tasks(worker_names: list[str] = None) -> list:
    # 1. First Get Schedule Classes
    schedules_retriever_classes = wrk.SCHEDULE_RETRIEVERS.items() if not worker_names else [
        (schedule_retriever_name, wrk.SCHEDULE_RETRIEVERS[schedule_retriever_name]) for schedule_retriever_name in
        worker_names]

    coros = [
        load_retriever(configure_game_retriever(source_name, retriever_class))
        for source_name, retriever_class in schedules_retriever_classes
    ]

    return coros


def update_batch_id():
    global BATCH_NUM, BATCH_DAY
    if BATCH_DAY != datetime.now().day:
        BATCH_DAY, BATCH_NUM = datetime.now().day, 0
    else:
        BATCH_NUM += 1


async def load_line_tasks(group: str, worker_name: str = None) -> list:
    if worker_group := wrk.LINE_WORKERS.get(group):
        # get all the bookmaker plugs to run
        line_retriever_classes = worker_group.items() if not worker_name else (worker_name, worker_group[worker_name])
        # create a batch id for this batch of lines
        batch_id = f"{str(datetime.now())}:{BATCH_NUM}"
        update_batch_id()

        coros = [
            load_retriever(configure_lines_retriever(batch_id, retriever_class))
            for source_name, retriever_class in line_retriever_classes
        ]

        return coros
