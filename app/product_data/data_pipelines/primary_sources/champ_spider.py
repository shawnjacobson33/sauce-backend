import json
from datetime import datetime

import cloudscraper


class ChampSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id

    def start_requests(self):
        url = 'https://core-api.champfantasysports.com/'
        headers = {
            "Host": "core-api.champfantasysports.com",
            "Accept": "*/*",
            "apollographql-client-version": "1.2.5-149",
            "version": "1.2.5",
            "subjectID": "66c3a326af8b5275e4b65bde",
            "Authorization": "Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImhDWGp3Nk9RRXJjVUxQSDRuUC1BSSJ9.eyJpc3MiOiJodHRwczovL2NoYW1wLXNwb3J0cy1jby51cy5hdXRoMC5jb20vIiwic3ViIjoic21zfDY2YzNhMzI2YWY4YjUyNzVlNGI2NWJkZSIsImF1ZCI6WyJodHRwczovL2NoYW1wLXNwb3J0cy1jby51cy5hdXRoMC5jb20vYXBpL3YyLyIsImh0dHBzOi8vY2hhbXAtc3BvcnRzLWNvLnVzLmF1dGgwLmNvbS91c2VyaW5mbyJdLCJpYXQiOjE3MjQwOTczMjIsImV4cCI6MTcyNjY4OTMyMiwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBwaG9uZSByZWFkOmN1cnJlbnRfdXNlciB1cGRhdGU6Y3VycmVudF91c2VyX21ldGFkYXRhIGRlbGV0ZTpjdXJyZW50X3VzZXJfbWV0YWRhdGEgY3JlYXRlOmN1cnJlbnRfdXNlcl9tZXRhZGF0YSBjcmVhdGU6Y3VycmVudF91c2VyX2RldmljZV9jcmVkZW50aWFscyBkZWxldGU6Y3VycmVudF91c2VyX2RldmljZV9jcmVkZW50aWFscyB1cGRhdGU6Y3VycmVudF91c2VyX2lkZW50aXRpZXMgb2ZmbGluZV9hY2Nlc3MiLCJndHkiOiJwYXNzd29yZCIsImF6cCI6Im5Cc05EMmY0NlZPZjNaSTNtc1VyaGpUZlRYQXE2bEo5In0.U36hVkDGBcZ6YJ2rMK5SDe4kHi0fceL3XmFIfWJx8cFvMF_0LZvHef-zviezss44VvqMOsOtlwSCQrqLoCxUV8gj3Apovm5KjJZfYXWsenLaJydHAGpmQx90VMLXRef7X3WyE4rMEGWrnEQWFeqkyFCY6jgddrnC2Fz6xxnzhLrNCgpeWHdaisqpF8FgIYqssSSxUN8Z4zLeT3CfjvZ9Ai8YTAnktbJYK06ImzHuQ7vCAxufMhy9t-VAuF5gFYF8Q1Lh0VmDo7iHEiz-J6VqAMhU3jmwXW96eHnZcontCEPhEBb85KO1NxTTVvouutsln13gCpyrAeTgUX5Z2t5PYA",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "X-APOLLO-OPERATION-TYPE": "query",
            "User-Agent": "App/149 CFNetwork/1496.0.7 Darwin/23.5.0",
            "apollographql-client-name": "com.champ.app-apollo-ios",
            "timeDiff": "-18000",
            "X-APOLLO-OPERATION-NAME": "ReadPicks"
        }

        for league in ['MLB', 'NFL', 'CFB', 'NBA', 'CBB', 'NHL']:
            data = {
                "operationName": "ReadPicks",
                "query": """query ReadPicks($sport: Sport, $cursor: String) {
          readPicks(sport: $sport, cursor: $cursor) {
            __typename
            ... on PicksPage {
              ...PicksScorePageData
            }
            ... on Error {
              message
            }
          }
        }
    
        fragment CompetitorInterfaceCore on CompetitorInterface {
          __typename
          sport
          shortName
          longName
          photo
          rank
          record
          untracked
        }
    
        fragment PickableData on PicksCompetitor {
          __typename
          ... on Player {
            ...PlayerCore
          }
        }
    
        fragment PicksBoostData on PicksBoost {
          __typename
          value
          type
          multiplier
        }
    
        fragment PicksPropData on PicksProp {
          __typename
          propID
          title
          value
          boost {
            __typename
            ...PicksBoostData
          }
        }
    
        fragment PicksScoreCompetitorData on PicksScoreCompetitor {
          __typename
          scoreID
          description
          competitor {
            __typename
            ...PickableData
          }
          props {
            __typename
            ...PicksPropData
          }
        }
    
        fragment PicksScoreData on PicksScore {
          __typename
          scoreID
          title
          competitors {
            __typename
            ...PicksScoreCompetitorData
          }
        }
    
        fragment PicksScorePageData on PicksPage {
          __typename
          title
          cursor
          emptyText
          items {
            __typename
            ...PicksScoreData
          }
        }
    
        fragment PlayerCore on Player {
          __typename
          ...CompetitorInterfaceCore
          playerID
          position
          number
          state
          team {
            __typename
            ...TeamCore
          }
        }
    
        fragment TeamCore on Team {
          __typename
          ...CompetitorInterfaceCore
          teamID
          favorite
        }""",
                "variables": {
                    "cursor": None,
                    "sport": league
                }
            }

            scraper = cloudscraper.create_scraper()
            response = scraper.post(url=url, headers=headers, json=data)
            if response.status_code == 200:
                self.parse_lines(response, league)
            else:
                print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_lines(self, response, league):
        # get body content in json format
        data = response.json()

        for event in data.get('items', []):
            game_info = event.get('title')
            for player in event.get('competitors', []):
                competitor = player.get('competitor')
                if competitor:
                    subject = competitor.get('longName')
                    position = competitor.get('position')
                    team = competitor.get('team')
                    if team:
                        player_team = team.get('shortName')


