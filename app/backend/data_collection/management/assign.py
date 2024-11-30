import schedule

from app.backend.database import MongoDB
from app.backend.data_collection.workers.utils import ActiveGames
import app.backend.data_collection.management.execute as exc


def assign_roster_tasks(worker_names: list[str] = None, interval: float = None) -> None:
    schedule.every(interval if interval else 86_400).seconds.do(exc.execute_roster_tasks, worker_names)


def assign_schedule_tasks(worker_names: list[str] = None, interval: float = None) -> None:
    schedule.every(interval if interval else 21_000).seconds.do(exc.execute_schedule_tasks, worker_names)


def there_are_active_games() -> bool:
    # This will run logic to delete any games that finished (no longer need game ids after they are done)
    if started_games := MongoDB.fetch_started_games():
        # store any currently running games in the data structure
        ActiveGames.update_active_games(started_games)
        return True

    return False


def assign_box_score_tasks(worker_names: list[str] = None, interval: float = None) -> None:
    if there_are_active_games():
        schedule.every(interval if interval else 120).seconds.do(exc.execute_box_score_tasks, worker_names)

    # TODO: need data stored somewhere to track if active games finished...then you can cancel the job


def assign_line_tasks(group: str, worker_names: list[str] = None, interval: float = None) -> None:
    # schedule the betting line tasks to be run for the inputted interval
    schedule.every(interval if interval else 120).seconds.do(exc.execute_line_tasks, group, worker_names)

