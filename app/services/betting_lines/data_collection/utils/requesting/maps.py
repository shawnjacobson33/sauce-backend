PAYLOAD_MAP = {
    'OddsShopper': {
        'urls': {
            'processing_info': 'https://www.oddsshopper.com/api/processingInfo/all',
            'matchups': 'https://www.oddsshopper.com/api/liveOdds/offers?league=all',
            'prop_lines': 'https://api.oddsshopper.com/api/offers/{}/outcomes/live?'
        },
        'headers': {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Origin': 'https://www.oddsshopper.com',
            'Referer': 'https://www.oddsshopper.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
        },
        'cookies': {
            '_gcl_au': '1.1.1623273209.1722653498',
            '_ga': 'GA1.1.578395929.1722653499',
            '_clck': 'sq3dhw%7C2%7Cfo0%7C0%7C1676',
            'advanced_ads_page_impressions': '%7B%22expires%22%3A2038059248%2C%22data%22%3A1%7D',
            'advanced_ads_browser_width': '1695',
            '_omappvp': '3R4fJFIbQqbzViRUPUXzhsn7uJbFjZ4ZycGfR5dL7cVUJS3AMw6TzfPLv0ZqsQycti3fiWi7kzDa8w4T9Tev789cLZG3Vzfq',
            '_hp2_id.2282576763': '%7B%22userId%22%3A%225144768874446519%22%2C%22pageviewId%22%3A%223326899841379432%22%2C%22sessionId%22%3A%221530461341951255%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
            '_hp2_ses_props.2436895921': '%7B%22ts%22%3A1722717777024%2C%22d%22%3A%22www.oddsshopper.com%22%2C%22h%22%3A%22%2Fliveodds%2Ftennis%22%7D',
            '_rdt_uuid': '1722653498619.4f5023ed-9e39-40e5-8323-7ed52f08d73c',
            '_clsk': 'ojlg6i%7C1722719433713%7C6%7C1%7Cw.clarity.ms%2Fcollect',
            '_ga_FF6BGPF4L9': 'GS1.1.1722717780.9.1.1722719608.0.0.0',
            '_ga_ZR0H6RP7T1': 'GS1.1.1722717778.9.1.1722719608.60.0.1437947439',
            '_hp2_id.2436895921': '%7B%22userId%22%3A%226206737379708108%22%2C%22pageviewId%22%3A%224229154290207751%22%2C%22sessionId%22%3A%228372167046692848%22%2C%22identity%22%3Anull%2C%22trackerVersion%22%3A%224.0%22%7D',
        },
        'params': {
            'prop_lines': {
                'state': 'NJ',
                'startDate': '{}',
                'endDate': '{}',
                'edgeSportsbooks': 'Circa,FanDuel,Pinnacle',
            }
        },
    },
    'BoomFantasy': {
        'urls': {
            'contest_ids': 'https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/graphql',
            'tokens': 'https://production-api.boomfantasy.com/api/v1/sessions',
            'prop_lines': 'https://production-boom-dfs-backend-api.boomfantasy.com/api/v1/contests/multiLine/{}'
        },
        'headers': {
            'prop_lines': {
                'Host': 'production-boom-dfs-backend-api.boomfantasy.com',
                'access-control-allow-origin': '*',
                'accept': 'application/json, text/plain, */*',
                'x-product-id': 'boom_dfs',
                'authorization': 'Bearer {}',
                'x-app-name': 'Boom',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'BoomDFS/3 CFNetwork/1568.100.1.2.1 Darwin/24.0.0',
                'x-device-id': 'D03577BA-B845-4E42-ADE3-59BB344E4AA9',
                'x-app-build': '3',
                'x-platform': 'ios',
            },
            'contest_ids': {
                'Host': 'production-boom-dfs-backend-api.boomfantasy.com',
                'content-type': 'application/json',
                'access-control-allow-origin': '*',
                'accept': 'application/json, text/plain, */*',
                'x-product-id': 'boom_dfs',
                'authorization': 'Bearer {}',
                'x-app-name': 'Boom',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'BoomDFS/1 CFNetwork/1568.200.51 Darwin/24.1.0',
                'x-device-id': 'D03577BA-B845-4E42-ADE3-59BB344E4AA9',
                'x-app-build': '1',
                'x-app-version': '32.2',
                'x-platform': 'ios',
            },
            'tokens': {
                'Host': 'production-api.boomfantasy.com',
                'content-type': 'application/json',
                'accept': 'application/json',
                'x-product-id': 'boom_dfs',
                'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXIiOjIsInR5cCI6ImFub24iLCJ0aWQiOiJib29tLWRmcyIsInBpZCI6ImJvb21fZGZzIiwiaWF0IjoxNjYxMjgyMjg3fQ.Cty9gHSp0dDdNR5iy1csmb98iSFdebBupQ4kRnGblI5irfdWlQScZ-BGEiHmUD7CCL3g8N5ji2vKkwAc40tQAHY0WDHpbCEJ9ebLZ0r5HfQeeBN9HjdYj9aCjkx_gtWbHBKQuoJ5zllZR2JG69G-ptxgtw8dvffiyfjphXOGdg7am6WnxcYNx4GiJkdrN1_IEcrKqyDNFmyQVV2hg1xDwa_Al7_YQZosYtgMU9rWMAGM3gz3nB2WlP_Fv9ZcJTvp65ZBSutSRxVWHu-sQze8WLh6VyTzHaUzGStrsTmiv0_i8fhoABerZvg7srkUp17ITwPWSd2LTcS--mhpI64IhSiF-Hqq_9yk6mGX7cs9c-dkiO0aSWrdtIrkscr6HgxtwaH8HQCOpRBfPq0ev_PABpnbGLy7lxJ6G3LPtq2si_vJyOnLfXX08qL93OexfTm-QYIglUAuoVJPRZaoRVhcTlw5Jkbp96HY763gRpmLhhp41IClJQI75UKXgTi937m-ZgSL2VE6ypd4xSkrl46LSchkzdf3jh3ArELsTGws9Fi_eY1-_ivcbJaZdM_tw0QE-sId1kyx2noQvyW8C4ETeAfmy_G3xkn_6tECV1dZ4ppMKzkXMr7o1dIOLraYYdwzDXRWzoyDQ4kujnGzGPoROSlzp3fdeoRpyEPcsiIbxwc',
                'x-app-name': 'Boom',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'BoomDFS/1 CFNetwork/1568.200.51 Darwin/24.1.0',
                'x-device-id': 'D03577BA-B845-4E42-ADE3-59BB344E4AA9',
                'x-app-build': '1',
                'x-app-version': '32.2',
                'x-platform': 'ios',
            }
        },
        'params': {
            'prop_lines': {
                'questionStatus': 'available',
            }
        },
        'json_data': {
            'contest_ids': {
                "query": "\n        query GetLobbyData(\n            $contestStatuses: [ContestStatus!]!\n            $imageType: ContestLobbyImageType!\n            $userId: ID\n        ) {\n            contests(statuses: $contestStatuses) {\n                _id\n                title\n                status\n                type\n                renderType\n                lobby(imageType: $imageType) {\n                    title {\n                        i18nKey\n                        additionalOptions\n                    }\n                    image {\n                        type\n                        path\n                        source\n                    }\n                }\n            }\n            lobbyHeroes(contestStatuses: $contestStatuses) {\n                type\n                status\n                priority\n                action\n                isUrlExternal\n                url\n                image {\n                    path\n                    source\n                    type\n                }\n                button {\n                    marginTop\n                    width\n                    height\n                }\n                contest {\n                    _id\n                    title\n                    type\n                    # navigation props\n                    renderType\n                    section\n                    league\n                    periodClassifier\n                    statistic\n                }\n            }\n            depositCTA {\n                image {\n                    type\n                    source\n                    path\n                }\n            }\n            depositHistory(userId: $userId) {\n                hasDeposited\n            }\n        }\n    ",
                "variables": {
                    "contestStatuses": ["active", "upcoming"],
                    "userId": "f69bbfae-6677-4075-a011-0fa48625da67",
                    "imageType": "wide"
                },
                "operationName": "GetLobbyData"
            },
            'tokens': {
                'authentication': {
                    'type': 'refresh',
                    'credentials': {
                        'refreshToken': 'f2349693266bff556fdb27180920733f438a73654e6e134696d75f53edb5c3c2a174de02fac6cebd7a0780aadf8a4b67b0979811ffb415aa400a850733a8a457',
                        'accessToken': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXIiOjMsInR5cCI6ImF1dGgiLCJ1aWQiOiJmNjliYmZhZS02Njc3LTQwNzUtYTAxMS0wZmE0ODYyNWRhNjciLCJzaWQiOiJkMjM1OTQyNy0zZjU1LTRkNGEtOTQ3ZC0xOTdlODc4ODZlYTEiLCJ0aWQiOiJib29tLWRmcyIsInBpZCI6ImJvb21fZGZzIiwicm9sZXMiOlsxMDMsMTA0XSwiY29ob3J0Ijo0MCwiZXhwIjoxNzM1NjYwMTg3LCJpYXQiOjE3MzU2NTk1ODd9.RdWtfjnoZx2kKvgHM85aYsxOnwf2GdTk3DPZu9fQU3UOoJaF-J8FI5SgmloWOE3C8OemOlSJGq0eLoyYN5egap9tlJSEcw8jXz6pmTNdJWFpfQq7aI3Llc8HdzzivjZgCoRWSdM3p8lc-R2khXT0u0-aeRVR12QX2yvA56vDImwY-wKNb0ZNTWm1ScIKB-BCcon9qR-EpK6z17Au_QQ4M2YcwU386du45SqOhPqk7SXH_luLNRsygWTWVFrBXzFv4bWihlvva62n_H8TGM9f22Lx2ZAh-UYmoEDjUZO7ygCyQdzKLbZMzl9lrm5XhfYUkmdYqow4JUb_2B5Q1Udq-KeZfd7TtdtA7eQc6wl7wRrLXCGeauNQxUth-nHCF-CdrsD3yyhMBDy3zp_6XQMejVRDkpu4xB61vO_LAKZc_9DtfWWNkoxTrj2R6tlblpUKue7ccRI1iN2Fsx41NgrCE59o1576zwzQOR3D7fwWVTgUC8Zc_LANhLPwWEmyrw9GAYlt82iUe_wXMwNci9QBqTVhQJNaF9wq6jPgWssNqDUsAaZY6z_aO772JnKOPKnEC2wrpu57gfHphKs-Q-Iq-2P-v5HihoQvk6IASf7Mn_b7VlLSvtx0jNJ7P1M4ASQV9Voc0j0qXd4W9UGee6-Omf1ceNK_lZs0l_JUcIGQKCc',
                    },
                },
                'eventInfo': {},
            }
        }
    }
}