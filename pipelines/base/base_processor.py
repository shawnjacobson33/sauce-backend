import functools
import time

import modin.pandas as pd

from db import db


def processor_logger(processing_func):
    @functools.wraps(processing_func)
    def wrapped(self, *args, **kwargs):
        print(f'[{self.domain}Pipeline] [Processing]: 🟢 Started Processing 🟢')
        start_time = time.time()
        result = processing_func(self, *args, **kwargs)
        end_time = time.time()
        self.times['processing_time'] = round(end_time - start_time, 4)
        print(f'[{self.domain}Pipeline] [Processing]: 🔴 Finished Processing 🔴\n'
              f'--------> ⏱️ {self.times['processing_time']} seconds ⏱️\n')

        db.pipeline_stats.add_processor_stats(self.times)

        return result

    return wrapped


class BaseProcessor:

    def __init__(self, domain: str, configs: dict, betting_lines: pd.DataFrame):
        self.domain = domain
        self.configs = configs

        self.times = {}

        self.ev_formula = self.configs['ev_formulas'][domain]

        if betting_lines.empty:
            raise ValueError('No player prop lines to process!')

        self.betting_lines_df = betting_lines
        self.betting_lines_df['impl_prb'] = (1 / self.betting_lines_df['odds']).round(3)

        self.sharp_betting_lines_df = self.betting_lines_df[
            (self.betting_lines_df['bookmaker'].isin(self.ev_formula['formula']))
        ]

    def _get_matching_betting_lines_mask(self, row):
        raise NotImplementedError

    def _get_devigged_lines(self) -> pd.DataFrame:

        def devig(row):
            matching_betting_lines = self.sharp_betting_lines_df[
                self._get_matching_betting_lines_mask(row) # Todo: is this the same for games lines also?
            ]

            if len(matching_betting_lines) == 1:
                matching_betting_line = matching_betting_lines.iloc[0]
                row['tw_prb'] = row['impl_prb'] / (matching_betting_line['impl_prb'] + row['impl_prb'])

            return row

        devigged_sharp_betting_lines_df = (self.sharp_betting_lines_df.apply(devig, axis=1).dropna(subset=['tw_prb']))
        return devigged_sharp_betting_lines_df

    def _weight_market_sharp_avgs(self, devigged_game_lines_df: pd.DataFrame) -> None:

        def weighted_market_avg(devigged_game_lines_df_grpd):
            weighted_market_avg_betting_line_df = (devigged_game_lines_df_grpd.iloc[[0]]
                                                    .drop(['bookmaker', 'odds', 'impl_prb'], axis=1))
            if len(devigged_game_lines_df_grpd) > 1:
                weights_sum = 0
                weighted_market_total = 0
                for _, row in devigged_game_lines_df_grpd.iterrows():  # Todo: slow
                    try:
                        weights = self.ev_formula['formula'][row['bookmaker']]
                        weights_sum += weights
                        weighted_market_total += weights * row['tw_prb']

                    except KeyError:
                        pass

                weighted_market_avg_betting_line_df['tw_prb'] = weighted_market_total / weights_sum

            return weighted_market_avg_betting_line_df

        self.sharp_betting_lines_df = (devigged_game_lines_df.groupby(['line', 'league', 'subject', 'market', 'label'])
                                                             .apply(weighted_market_avg))

    def _get_expected_values(self) -> pd.DataFrame:

        def ev(row):
            matching_sharp_betting_line = self.sharp_betting_lines_df[
                (self.sharp_betting_lines_df['line'] == row['line']) &
                (self.sharp_betting_lines_df['game'] == row['game']) &
                (self.sharp_betting_lines_df['subject'] == row['subject']) &
                (self.sharp_betting_lines_df['market'] == row['market']) &
                (self.sharp_betting_lines_df['label'] == row['label'])
            ]

            if len(matching_sharp_betting_line) == 1:
                prb_of_winning = matching_sharp_betting_line.iloc[0]['tw_prb']
                potential_winnings = row['odds'] - 1
                expected_value = (prb_of_winning * potential_winnings) - (1 - prb_of_winning)

                row['tw_prb'] = round(prb_of_winning, 3)
                row['ev'] = round(expected_value, 3)
                row['ev_formula'] = self.ev_formula['name']

            return row

        df = (self.betting_lines_df.apply(ev, axis=1)
                                   .sort_values(by='ev', ascending=False))
        return df

    @staticmethod
    def _cleanup_betting_lines(betting_lines_pr_list: list[dict]) -> None:
        for betting_line in betting_lines_pr_list:
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

    @processor_logger
    def run_processor(self) -> list[dict]:
        start_time = time.time()
        devigged_betting_lines_df = self._get_devigged_lines()
        end_time = time.time()
        self.times['devig_time'] = round(end_time - start_time, 4)

        start_time = time.time()
        self._weight_market_sharp_avgs(devigged_betting_lines_df)
        end_time = time.time()
        self.times['weighted_market_sharp_avgs_time'] = round(end_time - start_time, 4)

        start_time = time.time()
        betting_lines_df_pr = self._get_expected_values()
        end_time = time.time()
        self.times['ev_time'] = round(end_time - start_time, 4)

        betting_lines_pr_list = betting_lines_df_pr.to_dict(orient='records')
        self._cleanup_betting_lines(betting_lines_pr_list)
        return betting_lines_pr_list