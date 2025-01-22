from multiprocessing.queues import Queue

import modin.pandas as pd

from pipelines.base.base_processor import BaseProcessor


class GameLinesProcessor(BaseProcessor):

    def __init__(self, betting_lines_df: pd.DataFrame, configs: dict):
        super().__init__('Gamelines', betting_lines_df, configs)

    def _get_matching_betting_lines_mask(self, row) -> bool:
        return ((self.sharp_betting_lines_df['game'] == row['game']) &
                (self.sharp_betting_lines_df['market'] == row['market']) &
                (self.sharp_betting_lines_df['bookmaker'] == row['bookmaker']) &
                (self.sharp_betting_lines_df['subject'] != row['subject']))
