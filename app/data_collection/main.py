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


def configure(plug) -> bkm.BookmakerPlug:
    # get a database instance
    session = db.Database.get()
    # create a batch id for all betting lines in this run to use
    batch_id = str(uuid.uuid4())
    # get some attributes of the bookmaker from the database
    bookmaker_info = bkm.Bookmaker(db.get_bookmaker(session, plug.__name__))
    # create an instance of the bookmaker plug
    return plug(bookmaker_info, batch_id)


def save_pending_markets_to_file():
    # create a custom file path to store the betting lines sample
    file_path = f'utils/reports/general/pending_markets.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.Markets.get_pending_data(), f, indent=4)


def save_pending_subjects_to_file():
    # create a custom file path to store the betting lines sample
    file_path = f'utils/reports/general/pending_subjects.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.Subjects.get_pending_data(), f, indent=4)


def save_betting_lines_to_file():
    # create a custom file path to store the betting lines sample
    file_path = f'utils/reports/general/betting_lines.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(bkm.BettingLines.get(), f, indent=4)


def log_headers(bookmaker_plug: bkm.BookmakerPlug, header_type: str, headline: str) -> None:
    # log some template headers based on the input given
    bookmaker_plug.log(f'*** {header_type.upper()} *********************************************************************** {header_type.upper()} ***')
    bookmaker_plug.log(f'---------------------------------------> {header_type.upper()} {headline.upper()} <----------------------------------------')
    bookmaker_plug.log(f'*** {header_type.upper()} *********************************************************************** {header_type.upper()} ***')


def log_content(bookmaker_plug: bkm.BookmakerPlug, content_type: str, content: str) -> None:
    # log a template for different content based upon the input given
    bookmaker_plug.log(f'--- FOUND ------------------------> {content_type.upper()}: {content} <---------------------------- FOUND ---')


def cleanup(bookmaker_plug: bkm.BookmakerPlug):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            # track the time it takes to execute the function
            t1 = time.time()
            result = await func(*args, **kwargs)
            t2 = time.time()
            # console print a message
            print(f'[{bookmaker_plug.bookmaker_info.name}]: {bookmaker_plug.betting_lines_collected}, {round(t2-t1, 3)}s')
            # log different metrics
            log_headers(bookmaker_plug, header_type='metrics', headline='report')
            log_content(bookmaker_plug, content_type='metric', content=f'BATCH ID {bookmaker_plug.batch_id}')
            log_content(bookmaker_plug, content_type='metric', content=f'TIME {t2 - t1}s')
            log_content(bookmaker_plug, content_type='metric', content=f'# of BETTING LINES {bookmaker_plug.betting_lines_collected}')
            # save the pending markets, and subjects that were not found in the database to a file to be evaluated
            save_pending_markets_to_file()
            save_pending_subjects_to_file()
            # save the sample data of betting lines to a file for inspection
            save_betting_lines_to_file()
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

    print(f"[TOTAL]: {BettingLines.size()}, {round(t2-t1, 3)}s")

async def start_collecting(bookmaker_plug: bkm.BookmakerPlug):
    # a decorator to log metrics and save samples of the data in a file
    @cleanup(bookmaker_plug)
    async def collect():
        # wait until the plug is finished collecting its data
        await bookmaker_plug.collect()

        # log league reports
        log_headers(bookmaker_plug, header_type='leagues', headline='report')
        for league in bookmaker_plug.metrics.get_leagues_report():
            log_content(bookmaker_plug, content_type='league', content=league.upper())

        # log market reports
        log_headers(bookmaker_plug, header_type='markets', headline='report')
        for market, count in bookmaker_plug.metrics.get_markets_report().items():
            log_content(bookmaker_plug, content_type='market', content=f'{market} ({count})')

        # log subject reports
        log_headers(bookmaker_plug, header_type='subjects', headline='report')
        for subject, count in bookmaker_plug.metrics.get_subjects_report().items():
            log_content(bookmaker_plug, content_type='subject', content=f'{subject} ({count})')

    # wait until the collection process is done
    await collect()


if __name__ == '__main__':
    asyncio.run(run("", run_all=True))
