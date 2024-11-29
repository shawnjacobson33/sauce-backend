import schedule
import asyncio

from app.backend.data_collection.executing.load import load_line_tasks



def assign_line_tasks(group: str, worker_names: list[str] = None, interval: float = None):
    # section header
    print(f'{"*" * 22} Lines Retrieval {"*" * 22}\n')
    # get the coroutines
    line_tasks = load_line_tasks(group, worker_names)

    schedule.every(interval if interval else 120).seconds.do(asyncio.gather, *line_tasks)

