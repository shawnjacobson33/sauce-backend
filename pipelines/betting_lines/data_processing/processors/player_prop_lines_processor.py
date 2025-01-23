import modin.pandas as pd

from pipelines.base.base_processor import BaseProcessor


class PlayerPropsProcessor(BaseProcessor):
    """
    A processor class for handling player prop lines data.

    Inherits from:
        BaseProcessor: The base class for processing betting lines data.
    """

    def __init__(self, player_prop_lines: pd.DataFrame, configs: dict):
        """
        Initializes the PlayerPropsProcessor with the given parameters.

        Args:
            player_prop_lines (pd.DataFrame): The DataFrame containing player prop lines data.
            configs (dict): The configuration settings.
        """
        super().__init__('PlayerProps', player_prop_lines, configs)

    def _get_matching_betting_lines_mask(self, row) -> bool:
        """
        Gets the mask for matching betting lines.

        Args:
            row (pd.Series): The row of the DataFrame.

        Returns:
            bool: The mask indicating matching betting lines.
        """
        try:
            return ((self.betting_lines_df['line'] == row['line']) &
                    (self.betting_lines_df['game'] == row['game']) &
                    (self.betting_lines_df['subject'] == row['subject']) &
                    (self.betting_lines_df['market'] == row['market']) &
                    (self.betting_lines_df['bookmaker'] == row['bookmaker']) &
                    (self.betting_lines_df['label'] != row['label']))

        except Exception as e:
            self.log_message(message=f'Failed to get matching betting lines mask: {e}', level='EXCEPTION')