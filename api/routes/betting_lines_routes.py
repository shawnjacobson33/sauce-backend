from db import dev_db

from fastapi.routing import APIRouter


betting_lines_router = APIRouter()


@betting_lines_router.get("/betting_lines")
async def get_betting_lines():
    query = {
        'metrics.ev': {'$exists': True, '$ne': float('nan')},
    }
    proj = {
        'extra_source_stats': 0,
        'stream.batch_timestamp': 0,
        'stream.collection_timestamp': 0,
        'stream.odds': 0,
        'market_domain': 0,
        'date': 0,
    }
    betting_lines = await dev_db.betting_lines.get_betting_lines(
        query,
        proj,
        most_recent=True,
    )
    count = 0
    valid_betting_lines = []

    betting_lines = [betting_line for betting_line in betting_lines if betting_line['metrics']['ev'] != float('nan')]
    for betting_line in sorted(betting_lines, key=lambda x: x['metrics']['ev'], reverse=True):
        if count == 50:
            break

        game = betting_line.pop('game')
        betting_line['game'] = f'{game['away_team']} @ {game["home_team"]}'

        metrics = betting_line.pop('metrics')

        betting_line['ev'] = metrics['ev']
        valid_betting_lines.append(betting_line)
        count += 1

    return valid_betting_lines