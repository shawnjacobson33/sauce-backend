from multiprocessing.queues import Queue

import modin.pandas as pd  # Todo: consider switch to dask for parallel processing

from pipelines.base.base_processor import BaseProcessor


class PlayerPropsProcessor(BaseProcessor):

    def __init__(self, player_prop_lines: pd.DataFrame, configs: dict):
        super().__init__('PlayerProps', player_prop_lines, configs)

    def _get_matching_betting_lines_mask(self, row) -> bool:
        return ((self.sharp_betting_lines_df['line'] == row['line']) &
                (self.sharp_betting_lines_df['game'] == row['game']) &
                (self.sharp_betting_lines_df['subject'] == row['subject']) &
                (self.sharp_betting_lines_df['market'] == row['market']) &
                (self.sharp_betting_lines_df['bookmaker'] == row['bookmaker']) &
                (self.sharp_betting_lines_df['label'] != row['label']))
