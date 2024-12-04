import asyncio

from backend.app.database import MongoDB
import backend.app.data_collection.management.execute as exc
from backend.app.data_collection.workers.utils import ActiveGames
from backend.app.data_collection.management import reporting as rp


REPORTING_FUNC_MAP = {
    'execute_box_score_tasks': [rp.report_box_scores],
    'execute_schedule_tasks': [rp.report_games],
    'execute_line_tasks': [rp.report_lines, rp.report_line_worker_stats]
}


async def schedule_task(func, interval: int, *args):
    while True:
        await func(*args)
        for rp_func in REPORTING_FUNC_MAP.get(func.__name__, []):
            rp_func()

        await asyncio.sleep(interval)


async def assign_roster_tasks(worker_names: list[str] = None, interval: float = None) -> None:
    await schedule_task(exc.execute_roster_tasks, interval or 120)


async def assign_schedule_tasks(worker_names: list[str] = None, interval: float = None) -> None:
    await schedule_task(exc.execute_schedule_tasks, interval or 10)


def there_are_active_games() -> bool:
    # This will run logic to delete any games that finished (no longer need game ids after they are done)
    if started_games := MongoDB.fetch_started_games():
        # store any currently running games in the data structure
        ActiveGames.update_active_games(started_games)
        return True

    return False


async def assign_box_score_tasks(worker_names: list[str] = None, interval: float = None) -> None:
    if there_are_active_games():
        await schedule_task(exc.execute_box_score_tasks, interval or 120)

    # TODO: need data stored somewhere to track if active games finished...then you can cancel the job


async def assign_line_tasks(group: str, worker: str = None, interval: float = None) -> None:
    if group == 'B':
        await asyncio.sleep(int(interval / 2) if interval else 150)

    print('*' * 15, f'Group {group}', '*' * 15)
    await schedule_task(exc.execute_line_tasks, interval or 300, group, worker)
