import asyncio
import time

from app.db import db
from app.services.box_scores.data_collection import run_collectors
from app.services.utils import Standardizer


def _get_finished_games(games: list[dict]) -> list[dict]:
    return [game for game in games if db.games.is_game_finished(game)]


async def _cleanup_finished_games(games: list[dict]) -> None:
    game_ids = [game['_id'] for game in games]
    await db.betting_lines.update_betting_line_results(game_ids)
    await db.games.delete_games(game_ids)
    await db.box_scores.delete_box_scores(game_ids)


async def _check_for_finished_games(games: list[dict]) -> None:
    if finished_games := _get_finished_games(games):
        await _cleanup_finished_games(finished_games)

# Todo: if you are sleeping for an entire day make sure to release resources
async def run_pipeline():
    subjects = await db.subjects.get_subjects({})
    standardizer = Standardizer(subjects)
    while True:
        if live_games := await db.games.get_games({}, live=True):  # Poll for live games...TODO: more efficient ways to get live games?
            start_time = time.time()
            print('[BoxScores]: Running boxscores pipeline...')
            print('[BoxScores]: Starting data collectors...')
            collected_boxscores = await run_collectors(live_games, standardizer)
            print('[BoxScores]: Finished data collection...')
            print('[BoxScores]: Storing collected boxscores...')
            await db.box_scores.store_box_scores(collected_boxscores)
            print(f'[BoxScores]: Stored {len(collected_boxscores)} collected boxscores...')
            await db.betting_lines.update_live_stats(collected_boxscores)
            await _check_for_finished_games(live_games)
            end_time = time.time()
            print(f'[BoxScores]: Pipeline completed in {round(end_time - start_time, 2)} seconds. See you tomorrow...')
            await asyncio.sleep(30)


if __name__ == '__main__':
    asyncio.run(run_pipeline())