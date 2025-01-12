import time
import pandas as pd  # Todo: consider switch to dask for parallel processing

from app.services.betting_lines.data_processing.logging import processor_logger


class BettingLinesProcessor:

    def __init__(self, betting_lines_df: list[dict], ev_formula: dict[str, float]):
        self.betting_lines_df = pd.DataFrame(betting_lines_df)
        self.betting_lines_df['impl_prb'] = (1 / self.betting_lines_df['odds']).round(3)

        self.sharp_betting_lines_df = self.betting_lines_df[self.betting_lines_df['bookmaker'].isin(ev_formula.keys())]
        self.sharp_betting_lines_df['impl_prb'] = (1 / self.sharp_betting_lines_df['odds']).round(3)

        self.ev_formula = ev_formula

        self.times = dict()

    def _get_devigged_lines(self) -> pd.DataFrame:

        def devig(row):
            matching_prop_lines = self.sharp_betting_lines_df[
                (self.sharp_betting_lines_df['line'] == row['line']) &
                (self.sharp_betting_lines_df['league'] == row['league']) &
                (self.sharp_betting_lines_df['subject'] == row['subject']) &
                (self.sharp_betting_lines_df['market'] == row['market']) &
                (self.sharp_betting_lines_df['bookmaker'] == row['bookmaker']) &
                (self.sharp_betting_lines_df['label'] != row['label'])
            ]

            if len(matching_prop_lines) == 1:
                matching_prop_line = matching_prop_lines.iloc[0]
                row['tw_prb'] = row['impl_prb'] / (matching_prop_line['impl_prb'] + row['impl_prb'])

            return row

        df = self.sharp_betting_lines_df.apply(devig, axis=1).dropna()
        return df


    def _get_weighted_market_sharp_avgs(self, devigged_betting_lines_df: pd.DataFrame) -> pd.DataFrame:

        def weighted_market_avg(devigged_betting_lines_df_grpd):
            weighted_market_avg_betting_line_df = (devigged_betting_lines_df_grpd.iloc[[0]]
                                                    .drop(['bookmaker', 'odds', 'impl_prb'], axis=1))
            if len(devigged_betting_lines_df_grpd) > 1:
                weights_sum = 0
                weighted_market_total = 0
                for _, row in devigged_betting_lines_df_grpd.iterrows():
                    weights = self.ev_formula[row['bookmaker']]
                    weights_sum += weights
                    weighted_market_total += weights * row['tw_prb']

                weighted_market_avg_betting_line_df['tw_prb'] = weighted_market_total / weights_sum

            return weighted_market_avg_betting_line_df

        df = (devigged_betting_lines_df.groupby(['line', 'league', 'subject', 'market', 'label'])
                                         .apply(weighted_market_avg))
        return df


    def _get_expected_values(self):

        def ev(row):
            matching_sharp_prop_line = self.sharp_betting_lines_df[
                (self.sharp_betting_lines_df['line'] == row['line']) &
                (self.sharp_betting_lines_df['league'] == row['league']) &
                (self.sharp_betting_lines_df['subject'] == row['subject']) &
                (self.sharp_betting_lines_df['market'] == row['market']) &
                (self.sharp_betting_lines_df['label'] == row['label'])
            ]
            stats = dict()
            if len(matching_sharp_prop_line) == 1:
                prb_of_winning = matching_sharp_prop_line.iloc[0]['tw_prb']
                potential_winnings = row['odds'] - 1
                stats['tw_prb'] = prb_of_winning
                stats['ev'] = (prb_of_winning * potential_winnings) - (1 - prb_of_winning)
                stats['ev_formula'] = self.ev_formula['name']

            if tw_prb := stats.get('tw_prb', pd.NA):
                row['tw_prb'] = round(tw_prb, 3)

            if expected_value := stats.get('ev', pd.NA):
                row['ev'] = round(expected_value, 3)

            row['ev'] = stats.get('ev', pd.NA)
            row['ev_formula'] = stats.get('ev_formula', pd.NA)
            return row

        df = (
            self.betting_lines_df.apply(ev, axis=1)
                                 .sort_values(by='ev', ascending=False)
        )
        return df


    @processor_logger(message='Processing betting lines')
    def run_processor(self) -> list[dict]:
        start_time = time.time()
        devigged_betting_lines_df = self._get_devigged_lines()
        end_time = time.time()
        self.times['devig_time'] = round(end_time - start_time, 4)

        start_time = time.time()
        self.sharp_betting_lines_df = self._get_weighted_market_sharp_avgs(devigged_betting_lines_df)
        end_time = time.time()
        self.times['weighted_market_sharp_avgs_time'] = round(end_time - start_time, 4)


        start_time = time.time()
        betting_lines_df_pr = self._get_expected_values()
        end_time = time.time()
        self.times['ev_time'] = round(end_time - start_time, 4)

        betting_lines_df_pr.to_csv('data_processing/data-samples/oddsshopper-betting-lines-sample.csv', index=False)
        return betting_lines_df_pr.to_dict(orient='records')


if __name__ == '__main__':
    betting_lines_data = pd.read_csv('data-samples/oddsshopper-betting-lines-sample.csv').to_dict(orient='records')
    betting_lines_pr = run_processor(
        betting_lines_data,
        {'FanDuel': 0.65, 'BetOnline': 0.25, 'DraftKings': 0.1, 'name': 'sully'},
    )