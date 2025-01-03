
from app.prop_lines.data_collection import run_oddsshopper_scraper
from app.prop_lines.data_evaluation import run_ev_calculator


async def run_prop_lines_pipeline():
    betting_lines = await run_oddsshopper_scraper()
    evaluated_betting_lines = run_ev_calculator(betting_lines)
    return evaluated_betting_lines