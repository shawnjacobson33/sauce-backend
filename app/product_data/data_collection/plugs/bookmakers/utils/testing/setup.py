import json
import os
import time
import uuid
import asyncio

from app import db as database
from app.product_data.data_collection.plugs.data_hub import BettingLines
from app.product_data.data_collection.plugs.bookmakers.utils.modelling import Bookmaker, BookmakerPlug


def configure(plug) -> BookmakerPlug:
    # get a database instance
    db = database.Database.get()
    # create a batch id for all betting lines in this run to use
    batch_id = str(uuid.uuid4())
    # get some attributes of the bookmaker from the database
    bookmaker_info = Bookmaker(database.get_bookmaker(db, plug.__name__))
    # create an instance of the bookmaker plug
    return plug(bookmaker_info, batch_id)


def save_pending_markets_to_file(bookmaker_name: str):
    # create a custom file path to store the betting lines sample
    file_path = f'../reporting/reports/{bookmaker_name}/json_data/pending_markets.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(database.Markets.get_pending_data(), f, indent=4)


def save_pending_subjects_to_file(bookmaker_name: str):
    # create a custom file path to store the betting lines sample
    file_path = f'../reporting/reports/{bookmaker_name}/json_data/pending_subjects.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(database.Subjects.get_pending_data(), f, indent=4)


def save_betting_lines_to_file(bookmaker_name: str):
    # create a custom file path to store the betting lines sample
    file_path = f'../reporting/reports/{bookmaker_name}/json_data/betting_lines.json'
    # make any directories that don't already exist
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    file_path = os.path.join(os.path.dirname(__file__), file_path)
    with open(file_path, 'w') as f:
        # save the betting lines to the file, in pretty print mode
        json.dump(BettingLines.get(), f, indent=4)


def log_headers(bookmaker_plug: BookmakerPlug, header_type: str, headline: str) -> None:
    # log some template headers based on the input given
    bookmaker_plug.log(f'*** {header_type.upper()} *********************************************************************** {header_type.upper()} ***')
    bookmaker_plug.log(f'---------------------------------------> {header_type.upper()} {headline.upper()} <----------------------------------------')
    bookmaker_plug.log(f'*** {header_type.upper()} *********************************************************************** {header_type.upper()} ***')


def log_content(bookmaker_plug: BookmakerPlug, content_type: str, content: str) -> None:
    # log a template for different content based upon the input given
    bookmaker_plug.log(f'--- FOUND ------------------------> {content_type.upper()}: {content} <---------------------------- FOUND ---')


def cleanup(bookmaker_plug: BookmakerPlug):
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
            save_pending_markets_to_file(bookmaker_plug.bookmaker_info.name)
            save_pending_subjects_to_file(bookmaker_plug.bookmaker_info.name)
            # save the sample data of betting lines to a file for inspection
            save_betting_lines_to_file(bookmaker_plug.bookmaker_info.name)
            # return the function call as part of the decorator
            return result

        return wrapper

    return decorator


async def start(plug_class):
    # configure a bookmaker plug to collect data
    bookmaker_plug = configure(plug_class)

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


def run(plug_class):
    # execute the collection process of the plug asynchronously
    asyncio.run(start(plug_class))