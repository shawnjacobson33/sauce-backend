import asyncio
from datetime import datetime

import pandas as pd
from fastapi import APIRouter

from app.db import db
from app.services import run_betting_lines_pipeline


router = APIRouter()


@router.get('/start_betting_lines_pipeline')
async def start_betting_lines_pipeline():
    asyncio.create_task(run_betting_lines_pipeline())
    return {'message': 'Betting lines pipeline started.'}


@router.get('/betting_lines')
async def betting_lines():
    curr_betting_lines = await db.betting_lines.get_betting_lines()
    curr_betting_lines_df = pd.DataFrame(curr_betting_lines)
    curr_betting_lines_df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return (
        curr_betting_lines_df.head(20).to_dict(orient='records')
    )


