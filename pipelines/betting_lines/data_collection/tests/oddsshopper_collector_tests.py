import unittest
from datetime import datetime

from db import dev_db as db
from pipelines.configs import load_configs
from pipelines.utils import Standardizer
from pipelines.betting_lines.data_collection.collectors import OddsShopperCollector


class TestOddsShopperCollector(unittest.IsolatedAsyncioTestCase):

    @staticmethod
    async def _get_pre_reqs() -> Standardizer:
        configs = load_configs()
        teams = await db.teams.get_teams({})
        subjects = await db.subjects.get_subjects({})
        return Standardizer(configs['standardization'], teams, subjects)

    async def setUp(self):
        self.sample_matchups = {
            "offerCategories": [
                {
                    "name": "Gamelines",
                    "offers": [
                        {
                            "id": "ce80c942-ff54-464b-9581-f323d20cf692",
                            "name": "Point Spread",
                            "sportCode": "Basketball",
                            "leagueCode": "NBA",
                            "offerCategory": "Gamelines",
                            "eventType": "Game",
                            "betType": "Spread",
                            "participantType": "Team",
                            "labelType": "Participant"
                        },
                        {
                            "id": "2c9599d4-cc29-487b-bd5a-5018a0c94b4c",
                            "name": "Total",
                            "sportCode": "Basketball",
                            "leagueCode": "NBA",
                            "offerCategory": "Gamelines",
                            "eventType": "Game",
                            "betType": "Total",
                            "participantType": "Team",
                            "labelType": "OverUnder"
                        }
                    ]
                },
                {
                    "name": "PlayerProps",
                    "offers": [
                        {
                            "id": "aa4dd9a9-6aec-44f9-8ae0-3599520b9351",
                            "name": "Points",
                            "sportCode": "Basketball",
                            "leagueCode": "NBA",
                            "offerCategory": "PlayerProps",
                            "eventType": "Game",
                            "betType": "Total",
                            "participantType": "Player",
                            "labelType": "OverUnder"
                        },
                        {
                            "id": "550b8fc7-d7e7-46ab-8fa6-faf07ea98372",
                            "name": "3-Pointers",
                            "sportCode": "Basketball",
                            "leagueCode": "NBA",
                            "offerCategory": "PlayerProps",
                            "eventType": "Game",
                            "betType": "Total",
                            "participantType": "Player",
                            "labelType": "OverUnder"
                        },
                    ]
                }

            ]
        }
        self.sample_betting_lines = [
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
        configs, standardizer = await self._get_pre_reqs()

        self.obj = OddsShopperCollector(
            batch_timestamp=datetime.now(),
            betting_lines_container=[],
            standardizer=standardizer,
            configs=configs
        )

    #     def _get_offers(self, resp: dict) -> Iterable:
    #         offer_categories = resp.get('offerCategories', [])
    #         for offer_category in offer_categories:
    #             if (category_name := offer_category['name']) in self.configs['valid_market_domains']:
    #                 for offer in offer_category.get('offers', []):
    #                     offer['domain'] = category_name
    #                     yield offer

    def test_get_offers(self):
        offers = list(self.obj._get_offers(self.sample_matchups))
        self.assertEqual(len(offers), 4)
        self.assertEqual(offers[0]['name'], "Point Spread")
        self.assertEqual(offers[1]['name'], "Total")
        self.assertEqual(offers[2]['name'], "Points")
        self.assertEqual(offers[3]['name'], "3-Pointers")

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




# '_id': 'DraftKings:NBA:Rebounds + Assists:Kyle Kuzma:Under',
# 'batch_timestamp': '2025-01-21 20:13:00',
# 'bookmaker': 'DraftKings',
# 'collection_timestamp': '2025-01-21 20:13:10',
# 'date': '2025-01-21',
# 'extra_source_stats': {'hold': 0.043, 'tw_prb': 0.471},
# 'game': {
# '_id': 'NBA_20250121_WAS@LAL',
# 'away_team': 'WAS',
# 'game_time': '2025-01-21 22:30:00',
# 'home_team': 'LAL'
# },
# 'label': 'Under',
# 'league': 'NBA',
# 'line': 8.5,
# 'market': 'Rebounds + Assists',
# 'market_domain': 'PlayerProps',
# 'odds': 2.05,
# 'subject': 'Kyle Kuzma',
# 'url': 'https://sportsbook.draftkings.com/event/31695458?outcomes=0QA230349119%23137818903_15L42648Q1929971588Q20&referrer=singular_click_id%3D4d202b4d-1be4-419e-9281-8e2e35940d92&s=939763954 '}