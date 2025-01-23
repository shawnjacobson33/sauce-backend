from fastapi_app.main import app

from db import db


@app.get("/betting_lines")
async def get_betting_lines():
    query = {
        'metrics.ev': {'$exists': True},
    }
    proj = {
        'extra_source_stats': 0,
        'stream.batch_timestamp': 0,
        'stream.collection_timestamp': 0,
        'stream.odds': 0,
        'market_domain': 0,
        'date': 0,
    }
    betting_lines = await db.betting_lines.get_betting_lines(
        query,
        proj,
        most_recent=True,
        n=50
    )
    for betting_line in betting_lines:
        game = betting_line.pop('game')
        betting_line['game'] = f'{game['away_team']} @ {game["home_team"]}'

        metrics = betting_line.pop('metrics')
        betting_line['ev'] = metrics['ev']

    return betting_lines