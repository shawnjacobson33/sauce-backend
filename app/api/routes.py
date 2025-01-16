import asyncio

import pandas as pd
from fastapi import APIRouter

from app.db import db
from app.pipelines import run_services

router = APIRouter()


@router.get('/start_services')
async def start_services():
    asyncio.create_task(run_services())
    return {'message': 'Pipelines have started.'}


def _to_dict(**kwargs):
    return {k: v for k, v in kwargs.items() if v is not None}


@router.get('/betting_lines')
async def betting_lines():
    if most_recent_betting_lines := await db.betting_lines.get_betting_lines({}, most_recent=True):
        most_recent_betting_lines.sort(key=lambda x: x.get('ev'), reverse=True)  # Todo: set up an index in mongodb for this
        most_recent_betting_lines_df = pd.DataFrame(most_recent_betting_lines)
        return (
            most_recent_betting_lines_df.head(20).to_dict(orient='records')
        )
