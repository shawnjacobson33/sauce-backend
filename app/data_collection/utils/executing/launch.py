import time

from app.data_collection.utils.modelling.base_models import Retriever
from app.data_collection.utils.executing.configure import configure_schedule_retriever
from app.data_collection.utils.executing.definitions import SCHEDULE_RETRIEVERS, LINES_RETRIEVERS


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
    await collect()


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
        schedule_retriever = configure_schedule_retriever(source_name, schedule_retriever_class)
        # start collecting
        tasks.append(launch_retriever(schedule_retriever))
        