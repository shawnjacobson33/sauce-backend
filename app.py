import asyncio
import streamlit as st

from db import db

from pipelines.configs import load_configs
from pipelines import PipelineManager


async def display_betting_lines():
    while True:
        if betting_lines := await db.betting_lines.get_betting_lines({}):
            st.write(betting_lines)

        await asyncio.sleep(15)


async def run_app():
    configs = load_configs()
    pipeline_manager = PipelineManager(configs)

    await asyncio.gather(
        pipeline_manager.run_pipelines(),
        display_betting_lines()
    )


if __name__ == '__main__':
    asyncio.run(run_app())
