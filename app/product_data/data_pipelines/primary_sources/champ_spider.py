import asyncio
import json
import os
import time
import uuid
from datetime import datetime

from app.product_data.data_pipelines.request_management import AsyncRequestManager


class ChampSpider:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm

    async def start(self):
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

        tasks = []
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

            tasks.append(self.arm.post(url, self._parse_lines, league, headers=headers, json=data))

        await asyncio.gather(*tasks)

        relative_path = 'data_samples/champ_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[Champ]: {len(self.prop_lines)} lines')

    async def _parse_lines(self, response, league):
        # get body content in json format
        data = response.json().get('data', {}).get('readPicks', {})

        for event in data.get('items', []):
            game_info = event.get('title')
            for player in event.get('competitors', []):
                subject, position, subject_team, competitor = '', '', '', player.get('competitor')
                if competitor:
                    subject, position = competitor.get('longName'), competitor.get('position')
                    team = competitor.get('team')
                    if team:
                        subject_team = team.get('shortName')

                for prop in player.get('props', []):
                    market, line = prop.get('title'), prop.get('value')
                    labels, multiplier, boost = ['Over', 'Under'], None, prop.get('boost')
                    if boost:
                        multiplier = boost.get('multiplier')
                        labels = ['Under'] if multiplier < 1 else ['Over']

                    for label in labels:
                        self.prop_lines.append({
                            'batch_id': self.batch_id,
                            'time_processed': datetime.now(),
                            'league': league,
                            'game_info': game_info,
                            'market_category': 'player_props',
                            'market': market,
                            'subject_team': subject_team,
                            'subject': subject,
                            'position': position,
                            'bookmaker': 'Champ',
                            'label': label,
                            'line': line,
                            'multiplier': multiplier
                        })


async def main():
    spider = ChampSpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager())
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[Champ]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
