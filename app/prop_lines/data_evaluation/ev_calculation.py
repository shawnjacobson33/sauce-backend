import asyncio

import pandas as pd

from app.prop_lines.data_collection import run_oddsshopper_scraper


SHARP_PROP_BOOKMAKERS_WEIGHTS = {
    'BetOnline': 0.6,
    'FanDuel': 0.3,
    'Caesars': 0.1
}


async def _get_betting_lines() -> pd.DataFrame:
    betting_lines = await run_oddsshopper_scraper()
    return pd.DataFrame(betting_lines)
    # return pd.read_csv('../oddsshopper-betting-lines.csv')


def _get_true_prb(df: pd.DataFrame) -> pd.DataFrame:
    sharp_betting_lines = df[df['bookmaker'].isin(SHARP_PROP_BOOKMAKERS_WEIGHTS.keys())]

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
            row['true_prb'] = row['impl_prb'] / (matching_prop_line['impl_prb'] + row['impl_prb'])

        return row

    def weighted_market_avg(grouped_df):
        weighted_market_avg_betting_line_df = grouped_df.iloc[[0]].drop(['bookmaker', 'odds', 'impl_prb'], axis=1)
        if len(grouped_df) > 1:
            weights_sum = 0
            weighted_market_total = 0
            for i, row in grouped_df.iterrows():
                weights = SHARP_PROP_BOOKMAKERS_WEIGHTS[row['bookmaker']]
                weights_sum += weights
                weighted_market_total += weights * row['true_prb']

            weighted_market_avg_betting_line_df['true_prb'] = weighted_market_total / weights_sum

        return weighted_market_avg_betting_line_df

    devigged_betting_lines = sharp_betting_lines.apply(devig, axis=1).dropna()
    weighted_market_avg_betting_lines = (
        devigged_betting_lines.groupby(['line', 'league', 'subject', 'market', 'label'])
                              .apply(weighted_market_avg)
    )

    return weighted_market_avg_betting_lines


def _calculate_ev(betting_lines: pd.DataFrame, sharp_betting_lines: pd.DataFrame):
    non_sharp_betting_lines = (
        betting_lines[~betting_lines['bookmaker'].isin(SHARP_PROP_BOOKMAKERS_WEIGHTS.keys())]
    )

    def expected_value(row):
        matching_sharp_prop_line = sharp_betting_lines[
            (sharp_betting_lines['line'] == row['line']) &
            (sharp_betting_lines['league'] == row['league']) &
            (sharp_betting_lines['subject'] == row['subject']) &
            (sharp_betting_lines['market'] == row['market']) &
            (sharp_betting_lines['label'] == row['label'])
        ]
        if len(matching_sharp_prop_line) == 1:
            prb_of_winning = matching_sharp_prop_line.iloc[0]['true_prb']
            potential_winnings = row['odds'] - 1
            row['true_prb'] = prb_of_winning
            row['ev'] = round((prb_of_winning * potential_winnings) - (1 - prb_of_winning), 4)

        return row

    betting_lines_with_ev = (
        non_sharp_betting_lines.apply(expected_value, axis=1)
                               .dropna()
                               .sort_values(by='ev', ascending=False)
    )
    return betting_lines_with_ev


async def run_ev_calculator(betting_lines: pd.DataFrame = None):
    if not betting_lines:
        betting_lines = await _get_betting_lines()
    sharp_betting_lines = _get_true_prb(betting_lines)
    evaluated_betting_lines = _calculate_ev(betting_lines, sharp_betting_lines)
    return evaluated_betting_lines


if __name__ == '__main__':
    asyncio.run(run_ev_calculator())