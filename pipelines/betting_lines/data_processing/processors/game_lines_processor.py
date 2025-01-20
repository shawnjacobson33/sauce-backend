import pandas as pd  # Todo: consider switch to dask for parallel processing

from pipelines.base.base_processor import BaseProcessor


class GameLinesProcessor(BaseProcessor):

    def __init__(self, game_lines: pd.DataFrame, configs: dict):
        super().__init__('Gamelines', configs, game_lines)

    def _get_devigged_lines(self) -> pd.DataFrame:

        def devig(row):
            matching_game_lines = self.betting_lines_df[
                (self.betting_lines_df['line'] == row['line']) &
                (self.betting_lines_df['league'] == row['league']) &
                (self.betting_lines_df['subject'] == row['label']) &
                (self.betting_lines_df['market'] == row['market']) &
                (self.betting_lines_df['bookmaker'] == row['bookmaker']) &
                (self.betting_lines_df['label'] == row['subject'])
            ]

            if len(matching_game_lines) == 1:
                matching_game_line = matching_game_lines.iloc[0]
                row['tw_prb'] = row['impl_prb'] / (matching_game_line['impl_prb'] + row['impl_prb'])

            return row

        df = self.betting_lines_df.apply(devig, axis=1).dropna()
        return df


