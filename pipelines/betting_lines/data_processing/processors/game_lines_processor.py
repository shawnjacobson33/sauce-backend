import modin.pandas as pd

from pipelines.base.base_processor import BaseProcessor


class GameLinesProcessor(BaseProcessor):
    """
    A processor class for handling game lines data.

    Inherits from:
        BaseProcessor: The base class for processing betting lines data.
    """

    def __init__(self, betting_lines_df: pd.DataFrame, configs: dict):
        """
        Initializes the GameLinesProcessor with the given parameters.

        Args:
            betting_lines_df (pd.DataFrame): The DataFrame containing betting lines data.
            configs (dict): The configuration settings.
        """
        super().__init__('Gamelines', betting_lines_df, configs)

    def _get_matching_betting_lines_mask(self, row) -> bool:
        """
        Gets the mask for matching betting lines.

        Args:
            row (pd.Series): The row of the DataFrame.

        Returns:
            bool: The mask indicating matching betting lines.
        """
        try:
            return ((self.sharp_betting_lines_df['game'] == row['game']) &
                    (self.sharp_betting_lines_df['market'] == row['market']) &
                    (self.sharp_betting_lines_df['bookmaker'] == row['bookmaker']) &
                    (self.sharp_betting_lines_df['subject'] != row['subject']))

        except Exception as e:
            self.log_message(message=f'Failed to get matching betting lines mask for row: {row}', level='EXCEPTION')