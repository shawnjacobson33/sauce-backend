import json
import os
from typing import Dict, List


class Helper:
    def __init__(self, bookmaker: str):
        self.bookmaker = bookmaker

    def get_url(self, name: str = 'prop_lines'):
        mapping = {
            'BoomFantasy': {
                'prop_lines': 'https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/graphql'
            }, 'Champ': {
                'prop_lines': 'https://core-api.champfantasysports.com/'
            }, 'Dabble': {
                'competitions': 'https://api.dabble.com/competitions/active/',
                'events': 'https://api.dabble.com/competitions/{}/sport-fixtures',
                'prop_lines': 'https://api.dabble.com/sportfixtures/details/{}'
            }
        }

        return mapping.get(self.bookmaker).get(name)

    def get_headers(self):
        mapping = {
            'Champ': {
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
            }, 'Dabble': {
                'Host': 'api.dabble.com',
                'accept': 'application/json',
                'user-agent': 'Dabble/37 CFNetwork/1496.0.7 Darwin/23.5.0',
                'authorization': 'Bearer eyJraWQiOiJrNFhodFppYTFUcjZPdG1OQTJUQXhYaWlIMjhSV0hmdG9Fb21lVDRzbUhvPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhNzliOTc5ZS1lNWI2LTQ4MWEtODgwZi1lMTM4ODU1YTg4MjgiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0yLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMl8zcW44SFYydXkiLCJjbGllbnRfaWQiOiIyN2kwdDhvMzJjY2xxOWF0NzdpOWxubXQ3MCIsIm9yaWdpbl9qdGkiOiI4MWExMDcxZC1hYmM1LTQ2NDgtOWNmYi1mMGRkYjM5YmY1MmUiLCJldmVudF9pZCI6IjVlODcwZjQ0LTI5MzYtNDk0Yy05NmY2LTM0MzI1YTRjNmY2OSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4iLCJhdXRoX3RpbWUiOjE3MjI0NTU3NDksImV4cCI6MTcyNDEyMTE4NSwiaWF0IjoxNzI0MTE3NTg1LCJqdGkiOiJkMjNlMDc5Yi02MWYzLTRiZjQtYWQwZS0zOTQ3NzM4MGI4MmUiLCJ1c2VybmFtZSI6ImMyMDRjMDlhLTBiNDItNDgzZi1iMmM5LTdiY2M2ZmE1MWIzZSJ9.RPX0TgXU_A5kSg5mGbxmTnqbQSGgn6z9ls6RZHRwVRv38NyV41FlC_pNpoXOqQG_0qR57We1xRVj76PYSP-tEMg5WEoyIr3Z765uRFYW7XF71231xOA4ggr_dR0goJiUr5yY6G_33I1vO5Zo69N5-q39MgWMPxT89VhbR-1IfyocF-Xq68AO3Po7-LH7IRKVy64Kzlx9DpxWd73KjyHC_wjQmriCS2pWyOZGCGasZSdgrGxacrlwbdj-wJqxS3ere0OWSDHS97B5gPson9yruTyFKItZo5XWvDs1B3epIjN0ttJHRHQtdLcVjOCkbkm_GjcGxphD-_NCPzNphDhnYw',
                'accept-language': 'en-US,en;q=0.9',
            }
        }

        return mapping.get(self.bookmaker)

    def get_cookies(self):
        mapping = {
            'Dabble': {
                '__cf_bm': 'GqRmT59qXP4bcrTakr0U9arqC5aL_cYDP4z6c6pqJsU-1724117586-1.0.1.1-zVMLtycAgbyLZUP5nD_iyf4qGnB4Z0d4_XP4ChE1aGIy09l1G4qGnksw7POFGsTpeR_n9QcKjd_wOk7_Ae_Uww',
            }
        }

        return mapping.get(self.bookmaker)

    def get_params(self):
        mapping = {
            'Dabble': {
                'exclude[]': [
                    'markets',
                    'selections',
                    'prices',
                ],
            }
        }

        return mapping.get(self.bookmaker)

    def get_json_data(self, sport=None):
        mapping = {
            'BoomFantasy': {
                "query": """query EvergreenContest(
            $id: ID!
            $questionStatuses: [QuestionStatus!]!
            $overUnderSectionType: OverUnderSectionTypes
            $renderType: EvergreenContestRenderTypes
        ) {
            contest(
                id: $id
                questionStatuses: $questionStatuses
                overUnderSectionType: $overUnderSectionType
                renderType: $renderType
            ) {
                _id
                title
                renderType
                minLegs
                maxLegs
                minEntryFee
                payoutPerLegPerPrizeType
                minLegsPerPrizeType
                maxLegsPerPrizeType
                promotionalMaxEntryFee
                entryFeePerLegPerPrizeType
                restrictions {
                    _id
                }
                returnToLobbyOnSubmit
                sections {
                    type
                    leagues {
                        league
                        periodClassifier
                        gameFilters {
                            gameId
                            title {
                                i18nKey
                                additionalOptions
                            }
                            fullTitle {
                                i18nKey
                                additionalOptions
                            }
                            numQuestions
                        }
                        tabTitle {
                            i18nKey
                            additionalOptions
                        }
                        icon {
                            type
                            source
                            path
                        }
                        type
                        status
                        sections {
                            ... on SingleSectionType {
                                type
                                fullQuestions {
                                    __typename
                                    _id
                                    contestId
                                    renderType
                                    league
                                    statistic
                                    type
                                    periodClassifier
                                    title
                                    status
                                    sportsbookData
                                    searchTerms
                                    productIds
                                    pickSelectionTitle {
                                        i18nKey
                                        additionalOptions
                                    }
                                    statPopup {
                                        title {
                                            i18nKey
                                            additionalOptions
                                        }
                                        subTitle {
                                            i18nKey
                                            additionalOptions
                                        }
                                        table {
                                            i18nKey
                                            additionalOptions
                                        }
                                        body {
                                            i18nKey
                                            additionalOptions
                                        }
                                    }
                                    lockBehavior {
                                        type
                                        details {
                                            lockTime
                                        }
                                    }
                                    questionGrouping {
                                        identifier
                                        type
                                    }
                                    choices {
                                        _id
                                        valueAdjustment
                                        valueAdjustmentText {
                                            i18nKey
                                            additionalOptions
                                        }
                                        disabled
                                        pickSelectionTitle {
                                            i18nKey
                                            additionalOptions
                                        }
                                        questionTitle {
                                            i18nKey
                                            additionalOptions
                                        }
                                        gameInfo {
                                            i18nKey
                                            additionalOptions
                                        }
                                        game {
                                            _id
                                        }
                                        player {
                                            _id
                                            team {
                                                _id
                                            }
                                        }
                                        playerImage {
                                            abbreviation
                                            accentColor
                                            color
                                            image {
                                                type
                                                source
                                                path
                                            }
                                            jerseyNumber
                                            league
                                            secondaryColor
                                        }
                                        team {
                                            _id
                                        }
                                    }
                                }
                            }
                            ... on PlayerGroupingSectionType {
                                __typename
                                type
                                playerImage {
                                    abbreviation
                                    accentColor
                                    color
                                    image {
                                        type
                                        source
                                        path
                                    }
                                    jerseyNumber
                                    league
                                    secondaryColor
                                }
                                title {
                                    i18nKey
                                    additionalOptions
                                }
                                gameInfo {
                                    i18nKey
                                    additionalOptions
                                }
                                timeInfo {
                                    i18nKey
                                    additionalOptions
                                }
                                promoData {
                                    isPromo
                                    lockTime
                                }
                                playerId
                                teamId
                                gameId
                                periodClassifier
                                lockTime
                                fullQuestions {
                                    _id
                                    searchTerms
                                    pickSelectionTitle {
                                        i18nKey
                                        additionalOptions
                                    }
                                    isPromo
                                    statPopup {
                                        title {
                                            i18nKey
                                            additionalOptions
                                        }
                                        subTitle {
                                            i18nKey
                                            additionalOptions
                                        }
                                        table {
                                            i18nKey
                                            additionalOptions
                                        }
                                        body {
                                            i18nKey
                                            additionalOptions
                                        }
                                    }
                                    choices {
                                        _id
                                        type
                                        pickSelectionTitle {
                                            i18nKey
                                            additionalOptions
                                        }
                                        questionTitle {
                                            i18nKey
                                            additionalOptions
                                        }
                                    }
                                }
                            }
                            ... on GameGroupingSectionType {
                                type
                                collapsible
                                game {
                                    _id
                                    scheduled
                                    league
                                    homeTeam {
                                        _id
                                        league
                                        abbreviation
                                        color
                                    }
                                    awayTeam {
                                        _id
                                        league
                                        abbreviation
                                        color
                                    }
                                }
                                questions {
                                    _id
                                }
                            }
                        }
                    }
                }
                wheelsData {
                    layout {
                        value
                        start
                        end
                        weight
                        color
                    }
                    payoutTable {
                        header {
                            i18nKey
                            additionalOptions
                        }
                        rows {
                            i18nKey
                            additionalOptions
                        }
                    }
                }
            }
        }
    """,
                "variables": {
                    "id": "8ae8cea1-1e61-4524-a269-c053a1f938f9",
                    "questionStatuses": ["available"],
                    "overUnderSectionType": "playerGrouping",
                    "renderType": "evergreen"
                },
                "operationName": "EvergreenContest"
            },
            'Champ': {
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
                    "sport": sport
                }
            }
        }

        return mapping.get(self.bookmaker)

    def store(self, data) -> None:
        absolute_path = os.path.abspath(f'../data_samples/{self.bookmaker.lower()}_data.json')
        with open(absolute_path, 'w') as f:
            json.dump(data, f, default=str)

        print(f'[{self.bookmaker}]: {len(data)} lines')

        # self.plc.insert_many(self.prop_lines)
