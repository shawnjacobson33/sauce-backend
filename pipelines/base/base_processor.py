import functools
import time

import modin.pandas as pd

from db import db
from pipelines.utils.exceptions import ProcessingError


def processor_logger(processing_func):
    """
    A decorator that logs the start and end of the processing function,
    and records the processing time.

    Args:
        processing_func (function): The processing function to be decorated.

    Returns:
        function: The wrapped function with logging.
    """
    @functools.wraps(processing_func)
    def wrapped(self, *args, **kwargs):
        print('[BettingLinesPipeline] [Processing]: 🟢 Started Processing 🟢')
        start_time = time.time()
        result = processing_func(self, *args, **kwargs)
        self.times['processing_time'] = round(time.time() - start_time, 4)
        print('[BettingLinesPipeline] [Processing]: 🔴 Finished Processing 🔴')

        db.pipeline_stats.add_processor_stats(self.times)

        return result

    return wrapped


def time_execution(func):
    """
    A decorator that records the execution time of a function.

    Args:
        func (function): The function to be decorated.

    Returns:
        function: The wrapped function with execution time recording.
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs) -> pd.DataFrame | None:
        start_time = time.time()
        result = func(self, *args, **kwargs)
        elapsed_time = round(time.time() - start_time, 4)
        self.times[f'{func.__name__}_time'] = elapsed_time
        return result

    return wrapper


class BaseProcessor:
    """
    A base class for processing betting lines data.

    Attributes:
        name (str): The name of the processor.
        configs (dict): The configuration settings.
        times (dict): The dictionary to store processing times.
        ev_formula (dict): The expected value formula for the processor.
        betting_lines_df (pd.DataFrame): The DataFrame containing betting lines data.
        sharp_betting_lines_df (pd.DataFrame): The DataFrame containing sharp betting lines data.
    """

    def __init__(self, name: str, betting_lines_df: pd.DataFrame, configs: dict):
        """
        Initializes the BaseProcessor with the given parameters.

        Args:
            name (str): The name of the processor.
            betting_lines_df (pd.DataFrame): The DataFrame containing betting lines data.
            configs (dict): The configuration settings.

        Raises:
            ProcessingError: If there are no betting lines to process or no sharp betting lines to process.
        """
        self.name = name
        self.configs = configs

        self.times = {}

        self.ev_formula = self.configs['ev_formulas'][name]

        if betting_lines_df.empty:
            raise ProcessingError('No betting lines to process!')

        self.betting_lines_df = betting_lines_df.copy(deep=True)
        self.betting_lines_df = self.betting_lines_df[self.betting_lines_df['market_domain'] == self.name]
        self.betting_lines_df['impl_prb'] = (1 / self.betting_lines_df['odds']).round(3)

        self.sharp_betting_lines_df = self.betting_lines_df[
            (self.betting_lines_df['bookmaker'].isin(self.ev_formula['formula']))
        ]

        if self.sharp_betting_lines_df.empty:
            raise ProcessingError('No sharp betting lines to process!')

    def _get_matching_betting_lines_mask(self, row):
        """
        Gets the mask for matching betting lines.

        Args:
            row (pd.Series): The row of the DataFrame.

        Raises:
            NotImplementedError: This method should be implemented by subclasses.
        """
        raise NotImplementedError

    @time_execution
    def _get_devigged_lines(self) -> pd.DataFrame:
        """
        Gets the devigged lines from the sharp betting lines DataFrame.

        Returns:
            pd.DataFrame: The DataFrame containing devigged sharp betting lines.

        Raises:
            ProcessingError: If no sharp betting lines could be devigged.
        """
        def devig(row):
            complementing_betting_line = self.sharp_betting_lines_df[
                self._get_matching_betting_lines_mask(row)
            ]

            complementing_betting_line_size = complementing_betting_line.shape[0]

            if complementing_betting_line_size > 1:
                raise ProcessingError('While devigging more than one complementing betting line was found!')

            if complementing_betting_line_size == 1:
                row['tw_prb'] = row['impl_prb'] / (complementing_betting_line.iloc[0]['impl_prb'] + row['impl_prb'])

            return row

        try:
            devigged_sharp_betting_lines_df = (
                self.sharp_betting_lines_df.apply(devig, axis=1)
                                           .dropna(subset=['tw_prb'])
            )

            return devigged_sharp_betting_lines_df

        except Exception as e:
            raise ProcessingError(f'Failed to devig sharp betting lines: {e}')

    @time_execution
    def _weight_devigged_lines(self, devigged_game_lines_df: pd.DataFrame) -> pd.DataFrame | None:
        """
        Weights the devigged lines based on the expected value formula.

        Args:
            devigged_game_lines_df (pd.DataFrame): The DataFrame containing devigged game lines.

        Returns:
            pd.DataFrame | None: The DataFrame containing weighted devigged lines.
        """
        def weight_devigged_line(devigged_game_lines_df_grpd):
            weighted_sharp_betting_line = (
                devigged_game_lines_df_grpd.iloc[[0]]
                                            .drop(['bookmaker', 'odds', 'impl_prb'], axis=1)
            )

            if len(devigged_game_lines_df_grpd) > 1:
                weights_sum = 0
                weighted_market_total = 0
                for _, row in devigged_game_lines_df_grpd.iterrows():  # Todo: slow
                    weights = self.ev_formula['formula'][row['bookmaker']]
                    weights_sum += weights
                    weighted_market_total += weights * row['tw_prb']

                weighted_sharp_betting_line['tw_prb'] = weighted_market_total / weights_sum

            return weighted_sharp_betting_line

        try:
            weighted_sharp_betting_lines_df = (
                devigged_game_lines_df.groupby(['line', 'league', 'subject', 'market', 'label'])
                                      .apply(weight_devigged_line)
            )

            return weighted_sharp_betting_lines_df

        except Exception as e:
            raise ProcessingError(f'Failed to weight devigged lines: {e}')

    @time_execution
    def _get_expected_values(self, weighted_devigged_lines_df: pd.DataFrame) -> pd.DataFrame | None:
        """
        Calculates the expected values for the betting lines.

        Args:
            weighted_devigged_lines_df (pd.DataFrame): The DataFrame containing weighted devigged lines.

        Returns:
            pd.DataFrame | None: The DataFrame containing betting lines with expected values.

        Raises:
            ProcessingError: If no expected values could be calculated.
        """
        def ev(row):
            matching_sharp_betting_line = weighted_devigged_lines_df[
                (weighted_devigged_lines_df['line'] == row['line']) &
                (weighted_devigged_lines_df['league'] == row['league']) &
                (weighted_devigged_lines_df['subject'] == row['subject']) &
                (weighted_devigged_lines_df['market'] == row['market']) &
                (weighted_devigged_lines_df['label'] == row['label'])
            ]

            if len(matching_sharp_betting_line) == 1:
                prb_of_winning = matching_sharp_betting_line.iloc[0]['tw_prb']
                potential_winnings = row['odds'] - 1
                expected_value = (prb_of_winning * potential_winnings) - (1 - prb_of_winning)

                row['tw_prb'] = round(prb_of_winning, 3)
                row['ev'] = round(expected_value, 3)
                row['ev_formula'] = self.ev_formula['name']

            return row

        try:
            df = (
                self.betting_lines_df.apply(ev, axis=1)
                                     .sort_values(by='ev', ascending=False)
            )

            return df

        except KeyError as e:
            raise ProcessingError(f'Failed to calculate expected values: {e}')


    @staticmethod
    def _cleanup_betting_line(betting_line: dict) -> None:
        """
        Cleans up the betting line dictionary by removing unnecessary fields and organizing metrics.

        Args:
            betting_line (dict): The betting line dictionary.
        """
        try:
            if isinstance(betting_line['url'], float):
                del betting_line['url']

            if isinstance(betting_line['extra_source_stats'], float):
                del betting_line['extra_source_stats']

            metrics_dict = {
                'impl_prb': betting_line.pop('impl_prb')
            }

            if (tw_prb := betting_line.pop('tw_prb')) != float('NaN'):
                metrics_dict['tw_prb'] = tw_prb

            if (ev := betting_line.pop('ev')) != float('NaN'):
                metrics_dict['ev'] = ev

            if (ev_formula := betting_line.pop('ev_formula')) != float('NaN'):
                metrics_dict['ev_formula'] = ev_formula

            betting_line['metrics'] = metrics_dict

        except Exception as e:
            raise ProcessingError(f'Failed to cleanup betting line: {e}')

    @processor_logger
    def run_processor(self) -> list[dict]:
        """
        Runs the processor to calculate devigged lines, weighted devigged lines, and expected values.

        Returns:
            list[dict]: The list of processed betting lines.

        Raises:
            ProcessingError: If an error occurs during processing.
        """
        try:
            devigged_betting_lines_df = self._get_devigged_lines()

            weighted_devigged_lines_df = self._weight_devigged_lines(devigged_betting_lines_df)

            betting_lines_df_pr = self._get_expected_values(weighted_devigged_lines_df)

            betting_lines = betting_lines_df_pr.to_dict(orient='records')
            for betting_line in betting_lines:
                self._cleanup_betting_line(betting_line)

            return betting_lines

        except ProcessingError as e:
            self.log_message(message=f'Failed to process betting lines: {e}', level='ERROR')

        except Exception as e:
            self.log_message(message=f'Failed to process betting lines: {e}', level='EXCEPTION')

    def log_message(self, message: str, level: str = 'EXCEPTION'):
        """
        Logs a message with the specified level.

        Args:
            message (str): The log message.
            level (str, optional): The log level. Defaults to 'EXCEPTION'.
        """
        level = level.lower()

        if level == 'info':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ℹ️', message, 'ℹ️')

        if level == 'warning':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ⚠️', message, '⚠️')

        if level == 'error':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ‼️', message, '‼️')

        if level == 'exception':
            print(f'[BettingLinesPipeline] [Processing] [{self.name}]: ❌', message, '❌')