from pymongo.database import Database
import pandas as pd

# ****************** GOALS *********************

# 1. READ IN THE ALL PROP LINES FROM MOST RECENT BATCH
# - FROM A COLLECTION IN THE DATABASE

# 2. DEVIG FANDUEL, BETONLINE, OTHER SHARP BOOKMAKERS LINES TO GET THE "REAL ODDS" FOR A PARTICULAR LINE
# - GOING TO HAVE TO GROUP-BY BY MARKET, SUBJECT ID, AND LINE (NOT LABEL) TO GET THE ODDS FOR OVER AND UNDER

# 3. CALCULATE "EV" FOR EACH LINE BASED UPON THE "REAL ODDS" IN STEP 2
# - USE POLARS FOR OPTIMIZED PERFORMANCE OF GROUP-BY OPERATIONS.
# - SET UP LOGIC TO HANDLE WHEN THE SHARP BOOKMAKERS DON'T HAVE PARTICULAR LINES
# -- CALCULATE A MARKET AVERAGE?

# 4. STORE DATA OR PASS IT ALONG THE PIPE
# - BACK IN THE DATABASE?
# - RETURN IT?

from utils import SHARP_PROP_BOOKMAKERS


class DataEvaluator:
    def __init__(self, prop_lines: list[dict]):
        self.prop_lines = pd.DataFrame(prop_lines)
        self.prop_lines.set_index(['line', 'subject_id', 'market_id', 'bookmaker'], inplace=True)

    def _read_data(self):
        pass

    def _get_true_probs(self):
        # filter sharp lines
        sharp_lines = self.prop_lines[self.prop_lines['bookmaker'].isin(SHARP_PROP_BOOKMAKERS)]

        def devig(row):
            # get the complementing line
            corresponding_prop_line = sharp_lines.loc[(row['line'], row['subject_id'], row['market_id'], row['bookmaker'])]
            # If the row is FanDuel Over then there should only be a FanDuel Under in the frame.
            if (len(corresponding_prop_line) == 1) and (row['label'] != corresponding_prop_line.at[0, 'label']):
                # equation to remove the juice from the line
                row['fair_prob'] = row['implied_prob'] / (row['implied_prob'] + corresponding_prop_line.at[0, 'implied_prob'])

            return row

        return sharp_lines.apply(devig)

    def _calculate_ev(self):
        """
            Equation: EV = (P x W) - (1 - P) x L
            EV: Expected Value
            P: Probability of Winning
            W: Amount Won Per Dollar Bet (Odds - 1 in decimal format)
            L: Amount Lost Per Dollar Bet (typically 1, your stake)
        """

        sharp_lines = self._get_true_probs()

        def ev(row):
            # get the comparable lines
            corresponding_prop_lines = sharp_lines.loc[(row['line'], row['subject_id'], row['market_id'])]
            if not corresponding_prop_lines.empty:
                avg_true_prob = corresponding_prop_lines['fair_prob'].avg()
                # TODO: Need to first make method to get odds for DFS bookmakers like PrizePicks, Underdog Fantasy, etc.
                # POTENTIAL METHODS:
                # 1. Store a table somewhere with data on a bookmakers' best payout multiplier.
                # -- Then, instead of calculating/storing for every n-leg parlay that the bookmaker allows (which would
                # -- result in incredible storage costs), if a user wants to know how the 'ev' would be affected if they
                # -- wanted to make an n-leg parlay, those calculations can be made via an API call or in the frontend
                # -- based upon the 'ev' for the one payout multiplier. Make sure to store a field storing what the
                # -- value of 'n' is.
                row['ev'] = avg_true_prob * row['odds'] - (1 - avg_true_prob)

            return row

        # because the sharp bookmakers are being used as the true markers.
        non_sharp_prop_lines = self.prop_lines[~self.prop_lines['bookmaker'].isin(SHARP_PROP_BOOKMAKERS)]
        ev_calculated_prop_lines = non_sharp_prop_lines.apply(ev)
        # still want all the data, but need the newly updated data.
        return pd.concat([ev_calculated_prop_lines, sharp_lines])

    def evaluate(self):
        return self._calculate_ev()
