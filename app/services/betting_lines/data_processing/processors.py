import pandas as pd

from app.services.betting_lines.logging import data_processing_logger



@data_processing_logger(message='Devigging Sharp Betting Lines...')
def _get_devigged_lines(sharp_betting_lines_df: pd.DataFrame) -> pd.DataFrame:

    def devig(row):
        matching_prop_lines = sharp_betting_lines_df[
            (sharp_betting_lines_df['line'] == row['line']) &
            (sharp_betting_lines_df['league'] == row['league']) &
            (sharp_betting_lines_df['subject'] == row['subject']) &
            (sharp_betting_lines_df['market'] == row['market']) &
            (sharp_betting_lines_df['bookmaker'] == row['bookmaker']) &
            (sharp_betting_lines_df['label'] != row['label'])
        ]

        if len(matching_prop_lines) == 1:
            matching_prop_line = matching_prop_lines.iloc[0]
            row['tw_prb'] = row['impl_prb'] / (matching_prop_line['impl_prb'] + row['impl_prb'])

        return row

    df = sharp_betting_lines_df.apply(devig, axis=1).dropna()
    return df


@data_processing_logger(message='Calculating Weighted Market Sharp Avgs...')
def _get_weighted_market_sharp_avgs(devigged_betting_lines_df: pd.DataFrame, ev_formula: dict[str, float]) -> pd.DataFrame:

    def weighted_market_avg(devigged_betting_lines_df_grpd):
        weighted_market_avg_betting_line_df = (devigged_betting_lines_df_grpd.iloc[[0]]
                                                .drop(['bookmaker', 'odds', 'impl_prb'], axis=1))
        if len(devigged_betting_lines_df_grpd) > 1:
            weights_sum = 0
            weighted_market_total = 0
            for _, row in devigged_betting_lines_df_grpd.iterrows():
                weights = ev_formula[row['bookmaker']]
                weights_sum += weights
                weighted_market_total += weights * row['tw_prb']

            weighted_market_avg_betting_line_df['tw_prb'] = weighted_market_total / weights_sum

        return weighted_market_avg_betting_line_df

    df = (devigged_betting_lines_df.groupby(['line', 'league', 'subject', 'market', 'label'])
                                     .apply(weighted_market_avg))
    return df


def _get_sharp_betting_lines(df: pd.DataFrame, ev_formula: dict[str, float]) -> pd.DataFrame:
    sharp_betting_lines_df = df[df['bookmaker'].isin(ev_formula.keys())]
    devigged_sharp_betting_lines_df = _get_devigged_lines(sharp_betting_lines_df)
    weighted_market_sharp_avg_betting_lines_df = _get_weighted_market_sharp_avgs(
        devigged_sharp_betting_lines_df, ev_formula
    )
    return weighted_market_sharp_avg_betting_lines_df


@data_processing_logger(message='Calculating Expected Values...')
def _get_expected_values(betting_lines: pd.DataFrame, sharp_betting_lines: pd.DataFrame, ev_formula_name: str):

    def ev(row):
        matching_sharp_prop_line = sharp_betting_lines[
            (sharp_betting_lines['line'] == row['line']) &
            (sharp_betting_lines['league'] == row['league']) &
            (sharp_betting_lines['subject'] == row['subject']) &
            (sharp_betting_lines['market'] == row['market']) &
            (sharp_betting_lines['label'] == row['label'])
        ]
        stats = dict()
        if len(matching_sharp_prop_line) == 1:
            prb_of_winning = matching_sharp_prop_line.iloc[0]['tw_prb']
            potential_winnings = row['odds'] - 1
            stats['tw_prb'] = prb_of_winning
            stats['ev'] = (prb_of_winning * potential_winnings) - (1 - prb_of_winning)
            stats['ev_formula'] = ev_formula_name

        row['tw_prb'] = stats.get('tw_prb', pd.NA)
        row['ev'] = stats.get('ev', pd.NA)
        row['ev_formula'] = stats.get('ev_formula', pd.NA)
        return row

    df = (
        betting_lines.apply(ev, axis=1)
                     .sort_values(by='ev', ascending=False)
    )
    return df


def run_processors(betting_lines: list[dict], ev_formula: dict[str, float], ev_formula_name: str) -> list[dict]:
    betting_lines_df = pd.DataFrame(betting_lines)
    betting_lines_df['impl_prb'] = 1 / betting_lines_df['odds']
    sharp_betting_lines_df = _get_sharp_betting_lines(betting_lines_df, ev_formula)
    betting_lines_df_pr = _get_expected_values(betting_lines_df, sharp_betting_lines_df, ev_formula_name)
    betting_lines_df.to_csv('data_processing/data-samples/oddsshopper-betting-lines-sample.csv', index=False)
    return betting_lines_df_pr.to_dict(orient='records')


if __name__ == '__main__':
    betting_lines_data = pd.read_csv('data-samples/oddsshopper-betting-lines-sample.csv').to_dict(orient='records')
    betting_lines_pr = run_processors(
        betting_lines_data,
        {'FanDuel': 0.65, 'BetOnline': 0.25, 'DraftKings': 0.1},
        'sully'
    )