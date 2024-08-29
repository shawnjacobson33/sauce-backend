import json
from datetime import datetime

import cloudscraper


class DabbleSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.scraper = cloudscraper.create_scraper()
        self.headers = {
            'Host': 'api.dabble.com',
            'accept': 'application/json',
            'user-agent': 'Dabble/37 CFNetwork/1496.0.7 Darwin/23.5.0',
            'authorization': 'Bearer eyJraWQiOiJrNFhodFppYTFUcjZPdG1OQTJUQXhYaWlIMjhSV0hmdG9Fb21lVDRzbUhvPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhNzliOTc5ZS1lNWI2LTQ4MWEtODgwZi1lMTM4ODU1YTg4MjgiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0yLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMl8zcW44SFYydXkiLCJjbGllbnRfaWQiOiIyN2kwdDhvMzJjY2xxOWF0NzdpOWxubXQ3MCIsIm9yaWdpbl9qdGkiOiI4MWExMDcxZC1hYmM1LTQ2NDgtOWNmYi1mMGRkYjM5YmY1MmUiLCJldmVudF9pZCI6IjVlODcwZjQ0LTI5MzYtNDk0Yy05NmY2LTM0MzI1YTRjNmY2OSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4iLCJhdXRoX3RpbWUiOjE3MjI0NTU3NDksImV4cCI6MTcyNDEyMTE4NSwiaWF0IjoxNzI0MTE3NTg1LCJqdGkiOiJkMjNlMDc5Yi02MWYzLTRiZjQtYWQwZS0zOTQ3NzM4MGI4MmUiLCJ1c2VybmFtZSI6ImMyMDRjMDlhLTBiNDItNDgzZi1iMmM5LTdiY2M2ZmE1MWIzZSJ9.RPX0TgXU_A5kSg5mGbxmTnqbQSGgn6z9ls6RZHRwVRv38NyV41FlC_pNpoXOqQG_0qR57We1xRVj76PYSP-tEMg5WEoyIr3Z765uRFYW7XF71231xOA4ggr_dR0goJiUr5yY6G_33I1vO5Zo69N5-q39MgWMPxT89VhbR-1IfyocF-Xq68AO3Po7-LH7IRKVy64Kzlx9DpxWd73KjyHC_wjQmriCS2pWyOZGCGasZSdgrGxacrlwbdj-wJqxS3ere0OWSDHS97B5gPson9yruTyFKItZo5XWvDs1B3epIjN0ttJHRHQtdLcVjOCkbkm_GjcGxphD-_NCPzNphDhnYw',
            'accept-language': 'en-US,en;q=0.9',
        }
        self.cookies = {
            '__cf_bm': 'kxyL8EzK5BfUuTQWt2vOA2lLoHJi_G4GPCeIu4p2d4c-1724691701-1.0.1.1-NnmHR54TdhNA6NnaFtR7VKxWwCe3POlh.yDfQv_LFNng1fqzENyal0AWOrHgKU6rXUA..TnB9tS.ANX2aZu.pg',
        }

    def start_requests(self):
        url = 'https://api.dabble.com/competitions/active/'
        cookies = {
            '__cf_bm': 'GqRmT59qXP4bcrTakr0U9arqC5aL_cYDP4z6c6pqJsU-1724117586-1.0.1.1-zVMLtycAgbyLZUP5nD_iyf4qGnB4Z0d4_XP4ChE1aGIy09l1G4qGnksw7POFGsTpeR_n9QcKjd_wOk7_Ae_Uww',
        }

        response = self.scraper.get(url, headers=self.headers, cookies=cookies)
        if response.status_code == 200:
            self.parse_competitions(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_competitions(self, response):
        data = response.json().get('data')

        for competition in data.get('activeCompetitions', []):
            competition_id, league = competition.get('id'), competition.get('displayName')

            url = f'https://api.dabble.com/competitions/{competition_id}/sport-fixtures'
            params = {
                'exclude[]': [
                    'markets',
                    'selections',
                    'prices',
                ],
            }

            response = self.scraper.get(url, headers=self.headers, cookies=self.cookies, params=params)
            if response.status_code == 200:
                self.parse_events(response, league)
            else:
                print(f"Failed to retrieve {url} with status code {response.status_code}")

        with open('dabble_data.json', 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))

    def parse_events(self, response, league):
        data = response.json()

        for event in data.get('data', []):
            event_id, game_info, last_updated = event.get('id'), event.get('name'), event.get('updated')

            if event.get('isDisplayed'):
                url = f'https://api.dabble.com/sportfixtures/details/{event_id}'

                response = self.scraper.get(url, headers=self.headers, cookies=self.cookies)
                if response.status_code == 200:
                    self.parse_lines(response, league, game_info, last_updated)
                else:
                    print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response, league, game_info, last_updated):
        data = response.json().get('data')

        # get market groups
        markets = dict()
        for market_data in data.get('marketGroupMappings', []):
            market_name = market_data.get('name')
            if market_name:
                for market in market_data.get('markets', []):
                    market_id = market.get('id')
                    if market_id:
                        markets[market_id] = market_name

        for player_prop in data.get('playerProps', []):
            subject, subject_team = player_prop.get('playerName'), player_prop.get('teamAbbreviation')
            position, market = player_prop.get('position'), markets.get(player_prop.get('marketId'))
            label, line = player_prop.get('lineType').title(), player_prop.get('value')

            self.prop_lines.append({
                'batch_id': self.batch_id,
                'time_processed': datetime.now(),
                'last_updated': last_updated,
                'league': league,
                'game_info': game_info,
                'market_category': 'player_props',
                'market': market,
                'subject_team': subject_team,
                'subject': subject,
                'position': position,
                'bookmaker': 'Dabble',
                'label': label,
                'line': line
            })


DabbleSpider(batch_id='123').start_requests()
