import json
import os
import time
import uuid
import asyncio

from app import database as db
from app.data_collection import bookmakers as bkm
from app.data_collection.bookmakers import BettingLines


BOOKMAKER_PLUGS = {
    # "BetOnline": bkm.BetOnline,
    "BoomFantasy": bkm.BoomFantasy,
    "Dabble": bkm.Dabble,
    "Drafters": bkm.Drafters,
    "DraftKingsPick6": bkm.DraftKingsPick6,
    "HotStreak": bkm.HotStreak,
    "MoneyLine": bkm.MoneyLine,
    "OwnersBox": bkm.OwnersBox,
    "ParlayPlay": bkm.ParlayPlay,
    "Payday": bkm.Payday,
    "PrizePicks": bkm.PrizePicks,
    "Rebet": bkm.Rebet,
    "Sleeper": bkm.Sleeper,
    "SuperDraft": bkm.SuperDraft,
    "UnderdogFantasy": bkm.UnderdogFantasy,
    "VividPicks": bkm.VividPicks,
    "OddsShopper": bkm.OddsShopper
}


def configure(bookmaker_plug) -> bkm.BookmakerPlug:
    # get a database instance
    session = db.Database.get()
    # create a batch id for all betting lines in this run to use
    batch_id = str(uuid.uuid4())
    # get some attributes of the bookmaker from the database
    bookmaker_info = bkm.Bookmaker(db.get_bookmaker(session, bookmaker_plug.__name__))
    # create an instance of the bookmaker plug
    return bookmaker_plug(bookmaker_info, batch_id)


def output_bookmaker_job_stats(bookmaker_plug: bkm.BookmakerPlug, time_taken: float) -> None:
    # Because OddsShopper isn't actually a bookmaker, but a tool that holds other bookmaker's odds
    if bookmaker_plug.bookmaker_info.name == 'OddsShopper':
        # for every bookmaker that they offer
        for bookmaker in bkm.ODDSSHOPPER_NOVEL_BOOKMAKERS:
            # output the amount of lines collected from each bookmaker they offer and the time taken for the whole job.
            print(f'[{bookmaker}]: {bkm.BettingLines.size(bookmaker=bookmaker)}, {round(time_taken, 3)}s')
    else:
        # otherwise just output for the inputted bookmaker plug
        print(f'[{bookmaker_plug.bookmaker_info.name}]: {bookmaker_plug.betting_lines_collected}, {round(time_taken, 3)}s')


def get_file_path(entity_type: str, is_pending: bool) -> str:
    # get a customizable file path
    file_path = f'utils/reports/{"pending" if is_pending else "valid"}_{entity_type}.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # return file path
    return file_path


def save_valid_leagues_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='leagues', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.Leagues.get_valid_data(), f, indent=4)


def save_valid_markets_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='markets', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.Markets.get_valid_data(), f, indent=4)


def save_valid_subjects_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='subjects', is_pending=False)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.Subjects.get_valid_data(), f, indent=4)


def save_pending_markets_to_file() -> None:
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='markets', is_pending=True)
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.Markets.get_pending_data(), f, indent=4)


def save_pending_subjects_to_file():
    # create a custom file path to store the betting lines sample
    file_path = get_file_path(entity_type='subjects', is_pending=True)
    # open the pending subjects file
    with open(file_path, 'w') as f:
        # save the pending subjects to the file, in pretty print mode
        json.dump(bkm.Subjects.get_pending_data(), f, indent=4)


def save_betting_lines_to_file():
    # create a custom file path to store the betting lines sample
    file_path = 'utils/reports/betting_lines.json'
    # open the pending markets file
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.BettingLines.get(), f, indent=4)


def cleanup(bookmaker_plug: bkm.BookmakerPlug):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # track the time it takes to execute the function
            t1 = time.time()
            result = await func(*args, **kwargs)
            t2 = time.time()
            # output the statistics from the job
            output_bookmaker_job_stats(bookmaker_plug, t2-t1)
            # return the function call as part of the decorator
            return result

        return wrapper

    return decorator


async def run(plug: str, run_all: bool = False):
    # get all the bookmaker plugs to run
    plug_classes = BOOKMAKER_PLUGS.values() if run_all else [BOOKMAKER_PLUGS[plug]]
    # collect request task to run
    tasks = list()
    # for every bookmaker plug available
    for plug_class in plug_classes:
        # configure a bookmaker plug to collect data
        bookmaker_plug = configure(plug_class)
        # start collecting
        tasks.append(start_collecting(bookmaker_plug))

    # start making requests asynchronously
    t1 = time.time()
    await asyncio.gather(*tasks)
    t2 = time.time()
    # Output total number of betting lines collected and the time it took to run entire job
    print(f"[TOTAL]: {BettingLines.size()}, {round(t2-t1, 3)}s")
    # save the valid markets, leagues and subjects that were found in the database to a file to be evaluated
    save_valid_leagues_to_file()
    save_valid_markets_to_file()
    save_valid_subjects_to_file()
    # save the pending markets, and subjects that were not found in the database to a file to be evaluated
    save_pending_markets_to_file()
    save_pending_subjects_to_file()
    # save the sample data of betting lines to a file for inspection
    save_betting_lines_to_file()
    # output the size of the file storing the betting lines
    print(f"[FILE SIZE]: {round(os.path.getsize('utils/reports/betting_lines.json') / (1024 ** 2), 2)} MB")


async def start_collecting(bookmaker_plug: bkm.BookmakerPlug):
    # a decorator to log metrics and save samples of the data in a file
    @cleanup(bookmaker_plug)
    async def collect():
        # wait until the plug is finished collecting its data
        await bookmaker_plug.collect()

    # wait until the collection process is done
    await collect()


if __name__ == '__main__':
    asyncio.run(run("MoneyLine", run_all=False))
