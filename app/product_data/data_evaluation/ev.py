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

from utils import PROP_LINES_COLLECTION_NAME


class DataEvaluator:
    def __init__(self, db: Database):
        self.prop_lines = pd.DataFrame(db[PROP_LINES_COLLECTION_NAME])
        pass

    def _read_data(self):
        pass

    def _devig(self):
        sharp_bookmakers = ['FanDuel', 'Caesars', 'BetOnline']
        sharp_lines = self.prop_lines[self.prop_lines['bookmaker'].isin(sharp_bookmakers)]

        def custom_func(group):
            if len(group) == 2:
                
        grouped_lines = sharp_lines.groupby(by=['line', 'subject_id', 'market_id', 'league', 'bookmaker'])

    def _calculate_ev(self):
        pass

    def evaluate(self):
        pass
