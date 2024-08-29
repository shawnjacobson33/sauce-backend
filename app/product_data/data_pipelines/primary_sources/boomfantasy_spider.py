import json
from datetime import datetime

import cloudscraper


def get_headers(access_token):
    return {
        "Host": "production-boom-dfs-backend-api.boomfantasy.com",
        "content-type": "application/json",
        "access-control-allow-origin": "*",
        "accept": "application/json, text/plain, */*",
        "x-product-id": "sports_predictor",
        "authorization": f"Bearer {access_token}",
        "x-app-name": "Predictor",
        "accept-language": "en-US,en;q=0.9",
        "user-agent": "Predictor/1 CFNetwork/1496.0.7 Darwin/23.5.0",
        "x-device-id": "9498D138-1139-42BA-81A1-2E25990EA696",
        "x-app-build": "1",
        "x-app-version": "785",
        "x-platform": "ios"
    }


class BoomFantasySpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.scraper = cloudscraper.create_scraper()

        self.file_path = 'boomfantasy_tokens.txt'
        with open(self.file_path, 'r') as file:
            # Read all lines from the file
            lines = file.readlines()

            # Assign the first and second lines to variables
            self.access_token = lines[0].strip()
            self.refresh_token = lines[1].strip()

    def start_requests(self):
        url = "https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/graphql"
        headers = get_headers(self.access_token)
        data = {
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
        }

        response = self.scraper.post(url, headers=headers, json=data)
        if response.status_code == 200:
            self.parse_lines(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")
            print("Attempting to refresh tokens...")

            tokens_url = 'https://production-api.boomfantasy.com/api/v1/sessions'
            tokens_headers = {
                'Host': 'production-api.boomfantasy.com',
                'content-type': 'application/json',
                'accept': 'application/json',
                'x-product-id': 'sports_predictor',
                'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXIiOjIsInR5cCI6ImFub24iLCJ0aWQiOiJwcmVkaWN0b3IiLCJwaWQiOiJzcG9ydHNfcHJlZGljdG9yIiwiaWF0IjoxNjg3OTY1MjQwfQ.sKySbA5uKlb3TO1i9k1wlQSdhJeQfpB0H1G3VkqRRIA1IrJyxcwwB74PmP1r16_Zs00tP1QXYV2toBFLZDadaxxiI3XI9_n0r6K8DW4O71iEUz3lNZtYx3b890nNQySYG7RLLAQ4vpGEyktjxE9xSD4TE0jvTJa-nibp6s1d8Ncm7RNNb5Xkz0ugvp06wWcBAa9rZPuaMTlP4DtKdKewjFC6B4AlESdLRaEBYg8tNviyGJS4mcV8iEt3Zbd5B7XRMwUU90IvxbQKF1HSUGQW0qHIZEXgiw_HUxHs-9V4jM77r2hj0nwAIr6YlMweiNhZ09vMtTUcT6dWDEKDWyXGkMpZrzGlTuT9y5jCNNoNxmzrFwu4sFjKc4MXXu4A5vqWwe4KjbAZ7U9_f6m1DhdvsdPhgUaoqA19CUd0sVRcJCpccvdlA3qPcuP332tiVkZ8Pi2h4PyF2gS1WIwqfYAGb4VYmxgMqxFhmCKpk2oWDAB0908KkfvqAvtYxbH1s3uZORy0duZyWkQ7I7EnqsHnBayX6-SdhUocYrQJsUAWnQYcoJ9vOKwcV6je8lnQcMFaBOfHcbTmd9Vp92wEwPVsn1UtKlkhJHAy_bzMEzPPBH2e8fGNpkiSbF7Rkz5r5X5imV1PMI8ZuXmfQT_WJjmxp4RtaB2JEOXM8xSPSM4IDYg',
                'x-app-name': 'Predictor',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Predictor/1 CFNetwork/1496.0.7 Darwin/23.5.0',
                'x-device-id': '9498D138-1139-42BA-81A1-2E25990EA696',
                'x-app-build': '1',
                'x-app-version': '785',
                'x-platform': 'ios',
            }
            json_data = {
                'authentication': {
                    'type': 'refresh',
                    'credentials': {
                        'refreshToken': self.refresh_token,
                        'accessToken': self.access_token,
                    },
                },
                'eventInfo': {},
            }

            tokens_response = self.scraper.post(tokens_url, headers=tokens_headers, json=json_data)
            if tokens_response.status_code == 200:
                self.parse_tokens(tokens_response)

                response = self.scraper.post(url, headers=get_headers(self.access_token), json=data)
                if response.status_code == 200:
                    self.parse_lines(response)
                else:
                    print(f"Failed to retrieve {url} with status code {response.status_code}")
            else:
                print(f"Failed to retrieve {url} with status code {response.status_code}")

    def parse_tokens(self, response):
        data = response.json()

        self.access_token = data.get('accessToken')
        self.refresh_token = data.get('refreshToken')
        with open(self.file_path, 'w') as file:
            file.write(self.access_token + '\n')
            file.write(self.refresh_token + '\n')

    def parse_lines(self, response):
        data = response.json().get('data')

        contest = data.get('contest')
        if contest:
            for section in contest.get('sections', []):
                selection_type = section.get('type')
                # don't want the 'matchups' data
                if selection_type == 'overUnder':
                    for league in section.get('leagues', []):
                        league_name = league.get('league')
                        if league_name:
                            league_name = league_name.upper()
                        for league_section in league.get('sections', []):
                            game_time = league_section.get('lockTime')
                            if game_time:
                                # convert from unix to a datetime
                                game_time = datetime.fromtimestamp(game_time / 1000)

                            # get team
                            subject_team, player_image = '', league_section.get('playerImage')
                            if player_image:
                                subject_team = player_image.get('abbreviation')

                            # get subject
                            subject, title = '', league_section.get('title')
                            if title:
                                options = title.get('additionalOptions')
                                if options:
                                    first_name, last_name = options.get('firstName'), options.get('lastName')
                                    subject = ' '.join([first_name, last_name])

                            for question in league_section.get('fullQuestions', []):
                                # get line and market
                                line, market, pick_selection_title = '', '', question.get('pickSelectionTitle')
                                if pick_selection_title:
                                    additional_options = pick_selection_title.get('additionalOptions')
                                    if additional_options:
                                        line = additional_options.get('line')
                                        stat_text = additional_options.get('statText')
                                        if stat_text:
                                            stat_text_components = stat_text[0].split('.')
                                            if len(stat_text_components) == 4:
                                                market = stat_text_components[-2].lower().title()

                                for choice in question.get('choices', []):
                                    label = choice.get('type')
                                    if label:
                                        label = label.title()

                                    self.prop_lines.append({
                                        'batch_id': self.batch_id,
                                        'time_processed': datetime.now(),
                                        'league': league_name,
                                        'market_category': 'player_props',
                                        'market': market,
                                        'game_time': game_time,
                                        'subject_team': subject_team,
                                        'subject': subject,
                                        'bookmaker': 'BoomFantasy',
                                        'label': label,
                                        'line': line
                                    })

            with open('boomfantasy_data.json', 'w') as f:
                json.dump(self.prop_lines, f, default=str)

            print(len(self.prop_lines))


BoomFantasySpider(batch_id='abc').start_requests()
