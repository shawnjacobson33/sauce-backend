import asyncio

from backend.app.data_collection import management as mng
import backend.app.data_collection.management.execute as exc


async def manage_line_workers(group: str = None, worker: str = None, interval: int = None):
    if not group and not worker:
        coros = [
            mng.assign_line_tasks('A', interval=interval), mng.assign_line_tasks('B', interval=interval)
        ]
        await asyncio.gather(*coros)

    elif group and not worker:
        await mng.assign_line_tasks(group, interval=interval)

    elif group and worker:
        await mng.assign_line_tasks(group, worker, interval)



async def kickoff(lg_worker_names: list[str] = None, ln_args: dict = None) -> None:
    # first just get games
    # await exc.execute_schedule_tasks(lg_worker_names)
    # coros = [
    #     mng.assign_schedule_tasks(lg_worker_names),
    # ]
    # await
    # # run the roster retrieving tasks
    # mng.assign_roster_tasks(lg_worker_names)
    # run the schedule retrieving tasks
    await mng.assign_box_score_tasks(lg_worker_names)
    # # run the box score retrieving tasks
    #
    # # # run the lines retrieving tasks second
    # await manage_line_workers(**ln_args)
    # save all output data to json files
    # mng.generate_reports()


if __name__ == '__main__':
    asyncio.run(kickoff(None, {'interval': 150}))
