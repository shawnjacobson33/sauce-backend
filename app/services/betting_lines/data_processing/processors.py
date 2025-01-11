import asyncio
import pandas as pd


def _get_true_prb(df: pd.DataFrame, ev_formula: dict[str, float]) -> pd.DataFrame:
    sharp_betting_lines = df[df['bookmaker'].isin(ev_formula.keys())]

    def devig(row):
        matching_prop_lines = sharp_betting_lines[
            (sharp_betting_lines['line'] == row['line']) &
            (sharp_betting_lines['league'] == row['league']) &
            (sharp_betting_lines['subject'] == row['subject']) &
            (sharp_betting_lines['market'] == row['market']) &
            (sharp_betting_lines['bookmaker'] == row['bookmaker']) &
            (sharp_betting_lines['label'] != row['label'])
        ]

        if len(matching_prop_lines) == 1:
            matching_prop_line = matching_prop_lines.iloc[0]
            row['tw_prb'] = row['impl_prb'] / (matching_prop_line['impl_prb'] + row['impl_prb'])

        return row

    def weighted_market_avg(grouped_df):
        weighted_market_avg_betting_line_df = grouped_df.iloc[[0]].drop(['bookmaker', 'odds', 'impl_prb'], axis=1)
        if len(grouped_df) > 1:
            weights_sum = 0
            weighted_market_total = 0
            for _, row in grouped_df.iterrows():
                weights = ev_formula[row['bookmaker']]
                weights_sum += weights
                weighted_market_total += weights * row['tw_prb']

            weighted_market_avg_betting_line_df['tw_prb'] = weighted_market_total / weights_sum

        return weighted_market_avg_betting_line_df

    devigged_betting_lines = sharp_betting_lines.apply(devig, axis=1).dropna()
    print('[BettingLines]: Devigged sharp betting lines...')
    weighted_market_avg_betting_lines = (
              devigged_betting_lines.groupby(['line', 'league', 'subject', 'market', 'label'])
                                    .apply(weighted_market_avg)
    )
    print('[BettingLines]: Calculated weighted market average...')

    return weighted_market_avg_betting_lines


def _calculate_ev(betting_lines: pd.DataFrame, sharp_betting_lines: pd.DataFrame, ev_formula_name: str):

    def expected_value(row):
        matching_sharp_prop_line = sharp_betting_lines[
            (sharp_betting_lines['line'] == row['line']) &
            (sharp_betting_lines['league'] == row['league']) &
            (sharp_betting_lines['subject'] == row['subject']) &
            (sharp_betting_lines['market'] == row['market']) &
            (sharp_betting_lines['label'] == row['label'])
        ]
        if len(matching_sharp_prop_line) == 1:
            prb_of_winning = matching_sharp_prop_line.iloc[0]['tw_prb']
            potential_winnings = row['odds'] - 1
            row['tw_prb'] = prb_of_winning
            row['ev'] = (prb_of_winning * potential_winnings) - (1 - prb_of_winning)
            row['ev_formula'] = ev_formula_name

        return row

    betting_lines_with_ev = (
        betting_lines.apply(expected_value, axis=1)
                     .sort_values(by='ev', ascending=False)
    )
    print('[BettingLines]: Calculated expected values...')
    return betting_lines_with_ev


def run_processors(betting_lines: list[dict], ev_formula: dict[str, float], ev_formula_name: str) -> list[dict]:
    betting_lines_df = pd.DataFrame(betting_lines)
    betting_lines_df['impl_prb'] = 1 / betting_lines_df['odds']
    sharp_betting_lines_df = _get_true_prb(betting_lines_df, ev_formula)
    evaluated_betting_lines_df = _calculate_ev(betting_lines_df, sharp_betting_lines_df, ev_formula_name)
    return evaluated_betting_lines_df.to_dict(orient='records')


if __name__ == '__main__':
    betting_lines_data = pd.read_csv('data-samples/oddsshopper-betting-lines-sample.csv').to_dict(orient='records')
    asyncio.run(run_processors(betting_lines_data))