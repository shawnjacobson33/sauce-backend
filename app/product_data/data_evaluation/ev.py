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

from utils import PROP_LINES_COLLECTION_NAME, SHARP_PROP_BOOKMAKERS


class DataEvaluator:
    def __init__(self, db: Database):
        self.prop_lines = pd.DataFrame(db[PROP_LINES_COLLECTION_NAME])
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


    def evaluate(self):
        pass
