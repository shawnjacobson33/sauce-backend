import unittest
from datetime import datetime
from pipelines.utils import Standardizer
from pipelines.betting_lines.data_collection.collectors import OddsShopperCollector


class TestOddsShopperCollector(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.sample_data = [
            {
                "eventName": "Philadelphia 76ers @ Denver Nuggets",
                "offerName": "Point Spread",
                "startDate": "2025-01-22T03:00:00",
                "dateString": "Wednesday, January 22, 2025",
                "hold": 0.0390241269498569655097965132,
                "sportsbooks": [
                    "DraftKings", "FanDuel", "Caesars", "BetRivers", "BetMGM",
                    "ESPNBet", "Fanatics", "Fliff", "Rebet", "PointsBet",
                    "SugarHouse", "Bet365", "Pinnacle", "Unibet", "Circa",
                    "HardRock", "STNSports"
                ],
                "participants": [
                    {
                        "id": "aa5b188a-1980-4011-9fa2-e752bbe976b9",
                        "name": "Philadelphia 76ers",
                        "title": "76ers",
                        "abbreviation": "PHI",
                        "isHome": False,
                        "participantLogo": "https://awesemostorage.blob.core.windows.net/sports/nba/teams/PHI7.png",
                        "participantType": "Team"
                    },
                    {
                        "name": "@",
                        "isHome": False
                    },
                    {
                        "id": "1f97e477-b971-4b13-acc2-cfeddc265ade",
                        "name": "Denver Nuggets",
                        "title": "Nuggets",
                        "abbreviation": "DEN",
                        "isHome": True,
                        "participantLogo": "https://awesemostorage.blob.core.windows.net/sports/nba/teams/DENN.png",
                        "participantType": "Team"
                    }
                ],
                "sides": [
                    {
                        "label": "Philadelphia 76ers",
                        "bestOutcome": {
                            "id": "Outcomes/0000000002761092879-A",
                            "displayLabel": "+13.0",
                            "americanOdds": "-120",
                            "odds": 1.83333333
                        },
                        "outcomes": [
                            {"id": "Outcomes/0000000002761092879-A", "displayLabel": "+13.0", "americanOdds": "-120", "odds": 1.83333333},
                            {"id": "Outcomes/0000000002753179094-A", "displayLabel": "+12.5", "americanOdds": "-110", "odds": 1.90909091}
                        ]
                    }
                ]
            }
        ]

        self.obj = OddsShopperCollector(
            batch_timestamp=datetime.now(),
            betting_lines_container=[],

        )

    def test_event_name_parsing(self):
        event_names = self.obj.get_event_names()
        self.assertEqual(event_names, ["Philadelphia 76ers @ Denver Nuggets"])

    def test_offer_name_parsing(self):
        offer_names = self.obj.get_offer_names()
        self.assertEqual(offer_names, ["Point Spread"])

    def test_date_parsing(self):
        start_dates = self.obj.get_start_dates()
        self.assertEqual(start_dates, [datetime(2025, 1, 22, 3, 0, 0)])

    def test_sportsbooks_list(self):
        sportsbooks = self.obj.get_sportsbooks()
        self.assertIn("DraftKings", sportsbooks)
        self.assertIn("Bet365", sportsbooks)

    def test_participants_data(self):
        participants = self.obj.get_participants()
        self.assertEqual(len(participants), 3)
        self.assertEqual(participants[0]['name'], "Philadelphia 76ers")
        self.assertTrue(participants[2]['isHome'])

    def test_best_outcome(self):
        best_outcome = self.obj.get_best_outcome("Philadelphia 76ers")
        self.assertEqual(best_outcome['displayLabel'], "+13.0")
        self.assertEqual(best_outcome['americanOdds'], "-120")

    def test_outcomes(self):
        outcomes = self.obj.get_outcomes("Philadelphia 76ers")
        self.assertEqual(len(outcomes), 2)
        self.assertEqual(outcomes[1]['displayLabel'], "+12.5")
        self.assertEqual(outcomes[1]['americanOdds'], "-110")

if __name__ == '__main__':
    unittest.main()
