import asyncio
from threading import Timer

from app.backend.data_collection import management as mng


def manage_line_workers(worker_names: list[str]):
    mng.assign_line_tasks('A', worker_names, interval=120)
    Timer(40, mng.assign_line_tasks, args=('B', worker_names, 120))
    Timer(40, mng.assign_line_tasks, args=('C', worker_names, 120))
    mng.assign_line_tasks('D', worker_names, interval=120)


async def kickoff(lg_worker_names: list[str] = None, ln_worker_names: list[str] = None) -> None:
    # run the roster retrieving tasks
    mng.assign_roster_tasks(lg_worker_names)
    # run the schedule retrieving tasks
    mng.assign_schedule_tasks(lg_worker_names)
    # # run the box score retrieving tasks
    mng.assign_box_score_tasks(lg_worker_names)
    # # # run the lines retrieving tasks second
    manage_line_workers(ln_worker_names)
    # save all output data to json files
    mng.file_reports()


if __name__ == '__main__':
    asyncio.run(kickoff(None, ['BoomFantasy']))
