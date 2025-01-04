from fastapi import APIRouter
import pandas as pd
import asyncio


from app.services.betting_lines_service import run_prop_lines_pipeline


router = APIRouter()


@router.get('/start_pipeline')
async def start_pipeline():
    asyncio.create_task(run_prop_lines_pipeline())
    return {'message': 'Pipeline started'}


@router.get('/prop_lines')
async def prop_lines(league: str | None = None, bookmaker: str | None = None):
    evaluated_betting_lines = pd.read_csv('/app/services/betting_lines_service/evaluated_betting_lines.csv')
    if league:
        evaluated_betting_lines = evaluated_betting_lines[evaluated_betting_lines['league'] == league]

    if bookmaker:
        evaluated_betting_lines = evaluated_betting_lines[evaluated_betting_lines['bookmaker'] == bookmaker]

    return (
        evaluated_betting_lines.head(20).to_dict(orient='records')
    )