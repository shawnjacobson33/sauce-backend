import asyncio
import json
import os
import time
import uuid
from datetime import datetime
from uuid import UUID

from app.product_data.data_pipelines.request_management import AsyncRequestManager


class BoomFantasySpider:
    def __init__(self, batch_id: UUID, arm: AsyncRequestManager):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm

    async def start(self):
        url = "https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/graphql"
        json_data = {
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

        # get access tokens in order to make successful requests
        relative_path = 'primary_sources/boomfantasy_tokens.txt'
        absolute_path = os.path.abspath(relative_path)

        with open(absolute_path, 'r') as file:
            # Read all lines from the file
            lines = file.readlines()

            # Assign the first and second lines to variables
            access_token, refresh_token = lines[0].strip(), lines[1].strip()

        await self.arm.post_bf(url, self._parse_lines, absolute_path, refresh_token, access_token, json_data)

    async def _parse_lines(self, response):
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

            relative_path = 'data_samples/boomfantasy_data.json'
            absolute_path = os.path.abspath(relative_path)
            with open(absolute_path, 'w') as f:
                json.dump(self.prop_lines, f, default=str)

            print(f'[BoomFantasy]: {len(self.prop_lines)} lines')


async def main():
    spider = BoomFantasySpider(batch_id=uuid.uuid4(), arm=AsyncRequestManager())
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[BoomFantasy]: {round(end_time - start_time, 2)}s')

if __name__ == "__main__":
    asyncio.run(main())
