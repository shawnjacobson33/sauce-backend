import json
import os

from app.product_data.data_pipelines.utils.constants import IN_SEASON_LEAGUES


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
            }, 'Drafters': {
                'prop_lines': 'https://node.drafters.com/props-game/get-props-games'
            }, 'DraftKingsPick6': {
                'prop_lines': "https://pick6.draftkings.com/"
            },
            'HotStreak': {
                'prop_lines': 'https://api.hotstreak.gg/graphql'
            }, 'MoneyLine': {
                'prop_lines': 'https://moneylineapp.com/v3/API/v4/bets/all_available.php'
            }, 'OddsShopper': {
                'processing_info': 'https://www.oddsshopper.com/api/processingInfo/all',
                'matchups': 'https://www.oddsshopper.com/api/liveOdds/offers?league=all',
                'prop_lines': 'https://api.oddsshopper.com/api/offers/{}/outcomes/live?'
            }, 'OwnersBox': {
                'leagues': 'https://app.ownersbox.com/fsp-marketing/getSportInfo',
                'markets': 'https://app.ownersbox.com/fsp/marketType/active?',
                'prop_lines': 'https://app.ownersbox.com/fsp/v2/market?'
            }, 'ParlayPlay': {
                'prop_lines': 'https://parlayplay.io/api/v1/crossgame/search/?sport=All&league=&includeAlt=true'
            },
            'Payday': {
                'leagues': 'https://api.paydayfantasy.com/api/v2/app/contests/total',
                'contests': 'https://api.paydayfantasy.com/api/v2/app/contests',
                'prop_lines': 'https://api.paydayfantasy.com/api/v2/app/contests/{}/games'
            }, 'PrizePicks': {
                'leagues': 'https://api.prizepicks.com/leagues?game_mode=pickem',
                'prop_lines': 'https://api.prizepicks.com/projections?'
            }, 'Rebet': {
                'tourney_ids': 'https://api.rebet.app/prod/sportsbook-v3/get-new-odds-leagues',
                'prop_lines': 'https://api.rebet.app/prod/sportsbook-v3/load-sportsbook-data-v3'
            }, 'Sleeper': {
                'players': 'https://api.sleeper.app/players',
                'prop_lines': 'https://api.sleeper.app/lines/available?dynamic=true&include_preseason=true&eg=15.control'
            }, 'SuperDraft': {
                'prop_lines': 'https://api.superdraft.io/api/prop/v3/active-fantasy'
            },
            'ThriveFantasy': {
                'prop_lines': 'https://api.thrivefantasy.com/houseProp/upcomingHouseProps'
            }, 'UnderdogFantasy': {
                'teams': 'https://stats.underdogfantasy.com/v1/teams',
                'prop_lines': 'https://api.underdogfantasy.com/beta/v6/over_under_lines'
            }, 'VividPicks': {
                'prop_lines': 'https://api.betcha.one/v1/game/activePlayersForLeagueBoard'
            }
        }

        return mapping.get(self.bookmaker).get(name)

    def get_headers(self, name: str = 'prop_lines'):
        mapping = {
            'Champ': {
                'prop_lines': {
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

            }, 'Dabble': {
                'prop_lines': {
                    'Host': 'api.dabble.com',
                    'accept': 'application/json',
                    'user-agent': 'Dabble/37 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'authorization': 'Bearer eyJraWQiOiJrNFhodFppYTFUcjZPdG1OQTJUQXhYaWlIMjhSV0hmdG9Fb21lVDRzbUhvPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhNzliOTc5ZS1lNWI2LTQ4MWEtODgwZi1lMTM4ODU1YTg4MjgiLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0yLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMl8zcW44SFYydXkiLCJjbGllbnRfaWQiOiIyN2kwdDhvMzJjY2xxOWF0NzdpOWxubXQ3MCIsIm9yaWdpbl9qdGkiOiI4MWExMDcxZC1hYmM1LTQ2NDgtOWNmYi1mMGRkYjM5YmY1MmUiLCJldmVudF9pZCI6IjVlODcwZjQ0LTI5MzYtNDk0Yy05NmY2LTM0MzI1YTRjNmY2OSIsInRva2VuX3VzZSI6ImFjY2VzcyIsInNjb3BlIjoiYXdzLmNvZ25pdG8uc2lnbmluLnVzZXIuYWRtaW4iLCJhdXRoX3RpbWUiOjE3MjI0NTU3NDksImV4cCI6MTcyNDEyMTE4NSwiaWF0IjoxNzI0MTE3NTg1LCJqdGkiOiJkMjNlMDc5Yi02MWYzLTRiZjQtYWQwZS0zOTQ3NzM4MGI4MmUiLCJ1c2VybmFtZSI6ImMyMDRjMDlhLTBiNDItNDgzZi1iMmM5LTdiY2M2ZmE1MWIzZSJ9.RPX0TgXU_A5kSg5mGbxmTnqbQSGgn6z9ls6RZHRwVRv38NyV41FlC_pNpoXOqQG_0qR57We1xRVj76PYSP-tEMg5WEoyIr3Z765uRFYW7XF71231xOA4ggr_dR0goJiUr5yY6G_33I1vO5Zo69N5-q39MgWMPxT89VhbR-1IfyocF-Xq68AO3Po7-LH7IRKVy64Kzlx9DpxWd73KjyHC_wjQmriCS2pWyOZGCGasZSdgrGxacrlwbdj-wJqxS3ere0OWSDHS97B5gPson9yruTyFKItZo5XWvDs1B3epIjN0ttJHRHQtdLcVjOCkbkm_GjcGxphD-_NCPzNphDhnYw',
                    'accept-language': 'en-US,en;q=0.9',
                }

            }, 'Drafters': {
                'prop_lines': {
                    'Host': 'node.drafters.com',
                    'accept': '*/*',
                    'app_version': '13.7',
                    'if-none-match': 'W/"71e64-mEhxPh4ljtQEfs/ywHWFIsBrtwU"',
                    'accept-language': 'en-US,en;q=0.9',
                    'device_type': 'ios',
                    'user-agent': 'Drafters/1 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'device_id': 'cwEL85ss0EYPrM04yxrkGf:APA91bG2MtKk_iLZ-_twLz8QU_6CscuU3DQkPVw9m-6mNPTssieqvi51X6MCBlVfGkx4ygTdi7vGCT2WZsGRe_GbY8sO4Scx34WxTErCaXBQwWZNMlDiZu9YODqBqv5MrnXBVN_nX9_f',
                    'authorizations': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE3MjQwOTkxMzUsImp0aSI6Ik5qQmxaalU1TXpRNFlqVTRObUkyWldVMk0yVTRNV1k0TVRVM1l6YzFOVE13WldZeVlqTXdNbUkzTkdObVpXTTNPREZsT1RRMU1EVTRaRGhtT0RNME1BPT0iLCJpc3MiOiJodHRwczpcL1wvYXBpLmRyYWZ0ZXJzLmNvbVwvIiwibmJmIjoxNzI0MDk5MTI1LCJleHAiOjE3NTUyMDMxMjUsImRhdGEiOnsiaWQiOiI4MTUwMyIsInVzZXJuYW1lIjoidGhlcmVhbHNsaW0iLCJkcmFmdGVyc19pZCI6InRoZXJlYWxzbGltIiwiZW1haWwiOiJzaGF3bmphY29ic29uMzNAZ21haWwuY29tIn19.p86Dtjv0L-mraFMhbcvHMdHparpUsR1tXEymQcdwMrg',
                    'user_agent': 'iPhone 14 Pro',
                    'access_token': 'draft_user',
                }

            }, 'DraftKingsPick6': {
                'prop_lines': {
                    'accept': '*/*',
                    'accept-language': 'en-US,en;q=0.9',
                    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjU0NjgyNSIsImFwIjoiNjAxNDMxMzM3IiwiaWQiOiJmNmU0N2RjMzlkN2NjZDZkIiwidHIiOiI0NDM2MTQ5YjFhNzY3ZmRlMTg0MDZhZDE2ODAwY2YwMCIsInRpIjoxNzIzNjE1NzY3NzUzfX0=',
                    'priority': 'u=1, i',
                    'referer': 'https://pick6.draftkings.com/?sport=WNBA',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'traceparent': '00-4436149b1a767fde18406ad16800cf00-f6e47dc39d7ccd6d-01',
                    'tracestate': '546825@nr=0-1-546825-601431337-f6e47dc39d7ccd6d----1723615767753',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                }

            }, 'HotStreak': {
                'prop_lines': {
                    'Host': 'api.hotstreak.gg',
                    'accept': '*/*',
                    'content-type': 'application/json',
                    'authorization': 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJIb3RTdHJlYWsgKHByb2R1Y3Rpb24pIiwic3ViIjoiSHNmOjpVc2VyOnBydEpBNnciLCJleHAiOjE3MjcyMzQ4MjEsImlhdCI6MTcyNDU1NjQyMX0.ZCo5mvLX2DzDYQpBqXW2HG72kwl5EYC9VbXMKB6wRS8',
                    'x-requested-with': 'ios',
                    'user-agent': 'HotStreak/1717696638 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'accept-language': 'en-US,en;q=0.9',
                }

            },
            'MoneyLine': {
                'prop_lines': {
                    'Host': 'moneylineapp.com',
                    'Accept': 'application/json, text/plain, */*',
                    'User-Agent': 'MoneyLine/1 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'Accept-Language': 'en-US,en;q=0.9',
                }

            }, 'OddsShopper': {
                'prop_lines': {
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
                }

            }, 'OwnersBox': {
                'prop_lines': {
                    'Host': 'app.ownersbox.com',
                    'accept': 'application/json',
                    'content-type': 'application/json',
                    'user-agent': 'OwnersBox/145 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'ownersbox_version': '7.12.0',
                    'accept-language': 'en-US,en;q=0.9',
                    'ownersbox_device': 'ios',
                }

            }, 'ParlayPlay': {
                'prop_lines': {
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'X-ParlayPlay-Platform': 'web',
                    'X-Parlay-Request': '1',
                    'sec-ch-ua-mobile': '?0',
                    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Referer': 'https://parlayplay.io/?coupon=PP5&utm_campaign=ppc_brand&gclid=CjwKCAiA75itBhA6EiwAkho9e0WKfb9Q9CfMPQX3pItGGIyHcVBKOkTekc8rapMRzs75GX8hNshKcxoCUXsQAvD_BwE',
                    'X-Requested-With': 'XMLHttpRequest',
                    'X-CSRFToken': '1',
                    'sec-ch-ua-platform': '"macOS"',
                }

            }, 'Payday': {
                'prop_lines': {
                    'Host': 'api.paydayfantasy.com',
                    'accept': '*/*',
                    'content-type': 'application/json',
                    'user-agent': 'Betscoop/2 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJhdWQiOiI5NDJmYWIwZi0zMTQzLTQ2NmQtOTQ3MS1jODBmYmM3ZGYzMGUiLCJqdGkiOiI1OTZiMjMwOTczMGJiYjEyNjliNWU2MGM5MmJjMWFmMmFmYTY5MWFmYmFiZWQ5ZWI0OTIwN2JkYjQ3Yjg5Nzg0M2U4ZjQyZTg1MzUyNmFhNCIsImlhdCI6MTcyNDA5NzY0MC44ODQ5MTgsIm5iZiI6MTcyNDA5NzY0MC44ODQ5MjcsImV4cCI6MTc1NTYzMzY0MC44NzU1MjUsInN1YiI6IjBiODI2Mzc3LTRkMTUtNDBlMC1iNjZiLTU4ZGI3YTQzOWU1MCIsInNjb3BlcyI6WyIqIl19.qvwpWkgMa25A1R35YdAwiH00hkKur8xyo70JJYALVWMb2ig-BUDRfXXWrOC-CW7t3Pgf1hx9uZQNrq0P5MtI6FgLyUBoEf-RprxvXNV3SPukk8W42M8DSJAmsmRMnc99nsDLJyGv85v1VdTXe_eF5B9lqCZIwyCiVhS3Sc-_lkTP7Hm0yAXFIMqrt6nmjhcloRwV-S0tdyr6V9DQhnxGPU-9NasolMNf41FeYLcxVaynID67XJFs3rqFBQUR6PLzQLAgV_3vmwVXLJWvIgdrASomYcOUvBuKu4eiAPnd6SL5KTVPfwtY8yk2TmafTZUw0YTDU6EBYNlloBqpV6UJ0Kw4MZTwyP0lOyfQznOQu8jSUGrXujtfHMOHDT9lpFNe4Xdl91aNz-JJ-K-tSZEQlcNxRNfDC3wuhYzW6Mg1oBj10pIl3rHF9HYjVccdxuHBE9hkgSk3VpqEfxV9px0sGY3M0-8kfoWuCHSk5VVI4hlpdganx1vFJgkuFRpGC5yQAFle8siVaAQ9k9I-C84tLewCSGDkNHyO1PU_yJ_ivBs6Zdj1ZasSw5pLT6PQmROWaq28Xx_iKxS_mTmQxSlVjEM4mxuWV-xfuvUX2tSf8GJw_91QoGpN-7hTckwXygnxaVwQ96lIu4LbXnwveWKbZZDINYv8d_uk5SAzzFX1pwU',
                    'accept-language': 'en-US,en;q=0.9',
                }
            },
            'Rebet': {
                'tourney_ids': {
                    'Host': 'api.rebet.app',
                    'content-type': 'application/json',
                    'accept': 'application/json, text/plain, */*',
                    'baggage': 'sentry-environment=production,sentry-public_key=d4c222a7c513d9292ee6814277c6e1aa,sentry-release=com.rebet.app%408.97%2B399,sentry-trace_id=b6143e04f2ef4c4fa01c923aceca8cd8',
                    'accept-language': 'en-US,en;q=0.9',
                    'x-api-key': 'J9xowBQZM980G97zv9VoB9Ylady1pVtS5Ix9tuL1',
                    'sentry-trace': 'b6143e04f2ef4c4fa01c923aceca8cd8-69435e3a704f4230-0',
                    'user-agent': 'rebetMobileApp/399 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'x_api_key': 'J9xowBQZM980G97zv9VoB9Ylady1pVtS5Ix9tuL1',
                }, 'prop_lines': {
                    'Host': 'api.rebet.app',
                    'accept': 'application/json, text/plain, */*',
                    'content-type': 'application/json',
                    'authorization': 'Bearer eyJraWQiOiI3WkdkV1Y5THJucmdIY25QUWdMNWd0VzJXSGlpV2o3K2VBQ1FsR2FQeGlVPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJhOWE4NmE3My0yMTYwLTQyM2UtOTViOS02NTA0ZGI3ZTJjODciLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiYWRkcmVzcyI6eyJmb3JtYXR0ZWQiOiJNaW5uZWFwb2xpcywgTWlubmVzb3RhIn0sImJpcnRoZGF0ZSI6IjIwMDUtMDctMjciLCJpc3MiOiJodHRwczpcL1wvY29nbml0by1pZHAudXMtZWFzdC0yLmFtYXpvbmF3cy5jb21cL3VzLWVhc3QtMl9lcHo2ZEF4UXUiLCJwaG9uZV9udW1iZXJfdmVyaWZpZWQiOmZhbHNlLCJjb2duaXRvOnVzZXJuYW1lIjoidGhlcmVhbHNsaW0iLCJvcmlnaW5fanRpIjoiNDk2ZWJjMWItYTYyYS00ZjkxLWEwOTctNjRlODgyOGQ2ZTcxIiwiYXVkIjoiNzN2dnZsY2tkcmpqb3ZiaXNzM2loYmQyNm8iLCJldmVudF9pZCI6ImUyMWYyMDBkLTk5ZjMtNDM4Mi05YTk3LTUyOGNhYzgwOTY4ZiIsInRva2VuX3VzZSI6ImlkIiwiYXV0aF90aW1lIjoxNzI0NDI2ODAyLCJwaG9uZV9udW1iZXIiOiIrMTUwNzg4NDAyODYiLCJleHAiOjE3MjQ1MTMyMDIsImlhdCI6MTcyNDQyNjgwMiwianRpIjoiYzY3MWIwM2YtZWMzZC00ZWY0LWJkNjktNDkzZGUyZmFiNzJlIiwiZW1haWwiOiJzaGF3bmphY29ic29uMzNAZ21haWwuY29tIn0.F8YZwRmOADpUNuWmJ6wjFk-fZvGMopa0KM8kcf_vbvWv_Kik8czycPpERpNS1T8gtjcUUni12Qt9JjF15vSryaQScYw6pCft9q0yFrXjMNReIHgTNVNQQiFA6vrto2VdF4t4D7iyxyfU4dRdr95b2tpzm9VjsHNIKahqj76OrdWzjUQMLtBCqw-e7Wm9jm3TaMMTHlcjIefw0JUBiJlL9Lk_mqKtwKObX9tRTaqIS9XvXRvh7Hz_1W6btwZiEzx37ponn7L-AqnSArXTKDl_rzgN0qr8U4KBTCHdcJx5j5x8t2DeTrm-NKh3oF5AX4qasbiy77YFpLyBP2pVkkqVgw',
                    'sentry-trace': 'b6143e04f2ef4c4fa01c923aceca8cd8-69435e3a704f4230-0',
                    'baggage': 'sentry-environment=production,sentry-public_key=d4c222a7c513d9292ee6814277c6e1aa,sentry-release=com.rebet.app%408.97%2B399,sentry-trace_id=b6143e04f2ef4c4fa01c923aceca8cd8',
                    'user-agent': 'rebetMobileApp/399 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'accept-language': 'en-US,en;q=0.9',
                }
            }, 'Sleeper': {
                'prop_lines': {
                    'Host': 'api.sleeper.app',
                    'x-amp-session': '1724697278937',
                    'accept': 'application/json',
                    'authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdmF0YXIiOiIxNWQ3Y2YyNTliYzMwZWFiOGY2MTIwZjQ1ZjY1MmZiNiIsImRpc3BsYXlfbmFtZSI6IlNoYXdudGhlcmVhbHNoYWR5IiwiZXhwIjoxNzU2MjMzMzEyLCJpYXQiOjE3MjQ2OTczMTIsImlzX2JvdCI6ZmFsc2UsImlzX21hc3RlciI6ZmFsc2UsInJlYWxfbmFtZSI6bnVsbCwidXNlcl9pZCI6NzI5MjAyODc1NTk4Nzc0MjcyLCJ2YWxpZF8yZmEiOiJwaG9uZSJ9.hvc8FXdweWwNkBvrhCJ8ytRcBkX5ilDZa77IQtgleJM',
                    'x-api-client': 'api.cached',
                    'accept-language': 'en-US,en;q=0.9',
                    'user-agent': 'Sleeper/93.1.0 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'x-device-id': '71009696-F347-40AA-AE8C-5247A63041DF',
                    'x-platform': 'ios',
                    'x-build': '93.1.0',
                    'x-bundle': 'com.blitzstudios.sleeperbot',
                }
            }, 'SuperDraft': {
                'prop_lines': {
                    'Host': 'api.superdraft.io',
                    'content-type': 'application/json',
                    'accept': 'application/json',
                    'baggage': 'sentry-environment=production,sentry-public_key=a1c7747ba00849cab409e4e842041a1c,sentry-release=dfs-mobile%401.7.93%20%287%29,sentry-trace_id=c33fcdef6fa742f8966c3a4c780c53cd',
                    'timestamp': '2024-08-19T19:19:44.652Z',
                    'api-key': 'cont*177',
                    'device-type': '1',
                    'session-key': '6fb93f6f2c83a5091724081704',
                    'product-type': 'sd-dfs',
                    'sentry-trace': 'c33fcdef6fa742f8966c3a4c780c53cd-d5e48157b681463a-0',
                    'accept-language': 'en-US,en;q=0.9',
                    'user-agent': 'SuperDraft/7 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'datatype': 'json',
                    'attribution': '{"idfv":"6FB6DCDD-6CC1-46A7-B206-8AFAE050CE89","android_id":"","idfa":"00000000-0000-0000-0000-000000000000","gps_adid":"","messageId":"","templateId":"","campaignId":""}',
                    'geo-token': 'geoc84ee9df27b0e99a1724081704',
                }
            }, 'ThriveFantasy': {
                'prop_lines': {
                    'accept': 'application/json, text/plain, */*',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'origin': 'https://app.thrivefantasy.com',
                    'priority': 'u=1, i',
                    'referer': 'https://app.thrivefantasy.com/',
                    'sec-ch-ua': '"Chromium";v="128", "Not;A=Brand";v="24", "Google Chrome";v="128"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'token': 'eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJ0aGVyZWFsc2xpbSIsImF1ZGllbmNlIjoiSU9TIiwicGFzc3dvcmRMYXN0Q2hhbmdlZEF0IjpudWxsLCJpYXQiOjE3MjYwMjI1MDEsImV4cCI6MTcyNzc1MDUwMX0.HyH-Z73nMQi-fjSQhiFaxMNppLoxtYeEvVL9fp9K5wgjUYIIGyqR3mas_IZvhWBs0SWUZjRNb45Yqi1Ik0PDxw',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                    'x-tf-version': '1.1.39',
                }
            }, 'UnderdogFantasy': {
                'teams': {
                    'Host': 'stats.underdogfantasy.com',
                    'user-longitude': '-89.40836412683456',
                    'user-latitude': '43.070847054588',
                    'client-type': 'ios',
                    'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjNnRTM4R1FUTW1lcVA5djFYVllEUCJ9.eyJ1ZF9zdWIiOiJhNWFlYzNhNy05YWZhLTQ5NjQtODhmMC0yMDg3YjZlMmI3MjAiLCJ1ZF9lbWFpbCI6ImphY29ic29uc2hhd24zM0BnbWFpbC5jb20iLCJ1ZF91c2VybmFtZSI6InNsaW1zaGFkeTMzMyIsImlzcyI6Imh0dHBzOi8vbG9naW4udW5kZXJkb2dzcG9ydHMuY29tLyIsInN1YiI6ImF1dGgwfGE1YWVjM2E3LTlhZmEtNDk2NC04OGYwLTIwODdiNmUyYjcyMCIsImF1ZCI6WyJodHRwczovL2FwaS51bmRlcmRvZ2ZhbnRhc3kuY29tIiwiaHR0cHM6Ly91bmRlcmRvZy51bmRlcmRvZy5hdXRoMGFwcC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNzI1NDY5ODU2LCJleHAiOjE3MjU0NzM0NTYsInNjb3BlIjoib3BlbmlkIHByb2ZpbGUgZW1haWwgYWRkcmVzcyBwaG9uZSBvZmZsaW5lX2FjY2VzcyIsImd0eSI6InBhc3N3b3JkIiwiYXpwIjoiemZGMldIaHdzRkhEZzJUdnV1cmYzVHVPUVhOOGk1TXgifQ.kJcF9OLS5xBjYjMIFCy-9QaPCUfUzDOh9LSIcqsz-xILkKgn49w4dTGS0Zl6M9akvmPVl7coSyu_IeNF-c71bjFBMhXTF_YPg6yUFkUzgrHHT-NUr7VT9X0qjYOIOGt6wSK4P-efUuZKWhUQipQ7jiPGA6kjdfSmVrmE788ro2-3JYmjdKI7LAFaisxmzjAOn1ckby0_IJTKxh26nIYw_yyjOaaZIJvPrGkGsqUsGaLUQaV5MVc-HXa8rM0a2SYhJfifRRIGw8g4bre4ge6t4L2PvMzn3EWCYSW0mbgp-LSoYxnKnJe1MMwhvlhHJdvBEqOMgNgMe0hfyra336YKGQ',
                    'accept': '*/*',
                    'client-version': '1359',
                    'client-device-id': '7DE19B8B-D6D8-46A5-8339-F3F8960B9DA7',
                    'accept-language': 'en;q=1.0',
                    'client-request-id': 'D02F64DF-A4FB-4D84-99D7-1CB20C553EB3',
                    'user-agent': 'Underdog/24.08.07 (com.underdogsports.fantasy; build:1359; iOS 17.6.1) Alamofire/5.8.0',
                    'ud-user-id': 'a5aec3a7-9afa-4964-88f0-2087b6e2b720',
                }, 'prop_lines': {
                    'accept': 'application/json',
                    'accept-language': 'en-US,en;q=0.9',
                    'authorization': 'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IjNnRTM4R1FUTW1lcVA5djFYVllEUCJ9.eyJ1ZF9zdWIiOiJhNWFlYzNhNy05YWZhLTQ5NjQtODhmMC0yMDg3YjZlMmI3MjAiLCJ1ZF9lbWFpbCI6ImphY29ic29uc2hhd24zM0BnbWFpbC5jb20iLCJ1ZF91c2VybmFtZSI6InNsaW1zaGFkeTMzMyIsImlzcyI6Imh0dHBzOi8vbG9naW4udW5kZXJkb2dzcG9ydHMuY29tLyIsInN1YiI6ImF1dGgwfGE1YWVjM2E3LTlhZmEtNDk2NC04OGYwLTIwODdiNmUyYjcyMCIsImF1ZCI6Imh0dHBzOi8vYXBpLnVuZGVyZG9nZmFudGFzeS5jb20iLCJpYXQiOjE3MjM1ODI1NjgsImV4cCI6MTcyMzU4NjE2OCwic2NvcGUiOiJvZmZsaW5lX2FjY2VzcyIsImd0eSI6WyJyZWZyZXNoX3Rva2VuIiwicGFzc3dvcmQiXSwiYXpwIjoiY1F2WXoxVDJCQUZiaXg0ZFlSMzdkeUQ5TzBUaGYxczYifQ.Bq5SXeHZ_tujrI-VQR3m7rTdyAquLwUYaxPoqZdo5BMRuHB4tQU4_i77A69PGRBaeiJ6n6qCRc_PZkRyl-atdcDzr5ewYsdwCzexNCNUKcOuSnLK1rF2ELz-sUkm8hWrCclUqSDWGnnl9ywDpbnVqMQqa3fQKZG9OOPObK77IFR0khoSK7MN2IrKbhi7JDnRvD8Sc7_iEfusjvpAc0K5lRVXIfWdXKeG0m7g6InKJQLR55vNUicSbsA40dQvdQyZuP_oRE5TcPe95z4ji9-7ut-Gl9_Er9oKs-2tIRyVCLTbjl5Ig_JIH6CszaUh887fRpfJSAdaO8eHfPsxxLyvHw',
                    'client-device-id': '3ec5c852-be11-4848-a59c-a728187c7def',
                    'client-request-id': '4264d803-5c8e-4d1b-90ce-794d695324fa',
                    'client-type': 'web',
                    'client-version': '20240813192008',
                    'origin': 'https://underdogfantasy.com',
                    'priority': 'u=1, i',
                    'referer': 'https://underdogfantasy.com/',
                    'referring-link': '',
                    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"macOS"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
                    'user-latitude': '20.6968263',
                    'user-location-token': '',
                    'user-longitude': '-156.441985',
                }
            },
            'VividPicks': {
                'prop_lines': {
                    'Host': 'api.betcha.one',
                    'x-px-authorization': '3:813cbf65244904a5212f61f40a60ed86a6f2f1a951def7deb5a99ddcf2f01b67:y1vgcvZN6Ft0n1AuKpbq7ky2Y9uxN6fMF22sGO7mbzy3FM5SW0tUsdOsM2OCTSZuIvuhdxVkcANEEmFBzkwPZw==:1000:lfXRuGZ3/0l/vJ0slGB0923Os2OyFyKuXFu9awQ/FN6W9iJkNUDGuMwzq+J2glanORDV6OA1InID5OG1mIdPnvXomHwAi07a05zRAlDRrfsZGlfodXmmbDFXp+Arm+DCi8mz+iQGJPldNrx9fEtSFSyWOFFdBLkgjzhQDj0raeFi/bfwdV9F9CIegRaRNtNlWftBmdYZWUiwK9OyrrTngFqeVm+LkbLn+olbEAZspnk=',
                    'cache-control': 'no-cache',
                    'x-px-device-model': 'iPhone 14 Pro',
                    'x-px-device-fp': 'D9D02ACC-7330-4E54-BE30-81C3B80AA9CD',
                    'betcha-version': 'ios(17.5.1)/230/83512be3',
                    'betcha-timezone': 'America/Chicago',
                    'user-agent': 'VividPicks/230 CFNetwork/1496.0.7 Darwin/23.5.0',
                    'x-px-mobile-sdk-version': '3.0.3',
                    'betcha-device': 'D9D02ACC-7330-4E54-BE30-81C3B80AA9CD',
                    'baggage': 'sentry-environment=production,sentry-public_key=3da4c2e22ad24f418a283c246bb972a0,sentry-release=2.0.25%28230%29-83512be3,sentry-trace_id=2dc7d3ce69644329b9c80f91212dc785',
                    'x-px-os-version': '17.5.1',
                    'x-px-os': 'iOS',
                    'authorization': 'eyJraWQiOiJFeUpLSkNtYW9wdUw1VHpXZVVKNmd0aENXSTA3SUNoalUzejJ6ZEJLaE9NPSIsImFsZyI6IlJTMjU2In0.eyJzdWIiOiJmMTkyZmEwYi1iYWY0LTRiYWMtYWM3Zi02MDdhMTEwMDkyYzQiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMi5hbWF6b25hd3MuY29tXC91cy1lYXN0LTJfSXpnOHRIVnFhIiwiY29nbml0bzp1c2VybmFtZSI6ImphY29ic29uc2hhd24zMyIsInByZWZlcnJlZF91c2VybmFtZSI6ImphY29ic29uc2hhd24zMyIsImN1c3RvbTp1c2VySWQiOiI2NmFhOWI5MDFkYTRlN2ZjYTcxMGFmYWMiLCJhdWQiOiI1cDlrcDE4dXFmcGtndjExYzFtYWk2dDZqZiIsImV2ZW50X2lkIjoiZTk3ODVjZGYtMjIwOC00N2RhLThjNWYtNWJiMzg4ODAxMjBlIiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE3MjI0NTY5NjUsIl9pZCI6IjY2YWE5YjkwMWRhNGU3ZmNhNzEwYWZhYyIsImV4cCI6MTcyNDEyMjYwMCwiaWF0IjoxNzI0MTE5MDAwLCJ1c2VybmFtZSI6ImphY29ic29uc2hhd24zMyJ9.h6FSDRpUYZwOOPeULGWJYggW-_wGjPTUd0wDffgogbIXzKyKJZM4E7iJbOM2Z2BlUzPZnF5ckEUF-riAsHDrKlgdcXB1r0sCUK_uLO4rOgpqX0zA-KJbPb4a8OSOvsWm0ZjbGdGxamOBEW-uZlLOoJEnHeXbSDzGu1lnrU2g2496FjU70tYCQHmYOLOJ-pYR0pSDOgFiWqVbAbCt_Srz4PHkSpVWEIdR9BoKfwcbgCC9_uCBtSxt5HBBltY2_VR_XF1a0dZ5OxkEToPD6gncB1F0XD1O3hkmLJAzYPdjTeh2bB7Ho7GuPQVKuNXh9_mqEhq_ZW1iYAZX2gVmYIo3YA',
                    'accept-language': 'en-us',
                    'x-px-vid': '6fcc897e-4f79-11ef-81f2-7a4e1f9004de',
                    'accept': 'application/json',
                    'content-type': 'application/json',
                    'x-px-uuid': 'de460224-5e97-11ef-ac2f-ad7d00d797ac',
                    'sentry-trace': '2dc7d3ce69644329b9c80f91212dc785-08ef37f394df43a6-0',
                }

            }
        }

        return mapping.get(self.bookmaker).get(name)

    def get_cookies(self):
        mapping = {
            'Dabble': {
                '__cf_bm': 'GqRmT59qXP4bcrTakr0U9arqC5aL_cYDP4z6c6pqJsU-1724117586-1.0.1.1-zVMLtycAgbyLZUP5nD_iyf4qGnB4Z0d4_XP4ChE1aGIy09l1G4qGnksw7POFGsTpeR_n9QcKjd_wOk7_Ae_Uww',
            }, 'MoneyLine': {
                'PHPSESSID': '3kndhe1s5enl10i7q25klikeek',
            }, 'OddsShopper': {
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
            }, 'OwnersBox': {
                'obauth': 'eyJhbGciOiJIUzUxMiJ9.eyJvYnRva2VuIjoiMFE2SzJRMVkxTjhEIiwidXNlclN0YXRlIjoiQUNUSVZFIiwiaXNzIjoiT3duZXJzQm94IiwidmVyaWZpZWQiOmZhbHNlLCJ0b2tlbkV4cGlyeSI6MTcyNDEyMDAxMjc4MCwic2Vzc2lvbklkIjozNDE5MDU1MDk2fQ.9dcOi9DJ8_R1PTD4-m3VqXebAj1pZ0LAFzaXsaIGkIsvrjLOjF9jW5KNQEoDYOKjLhyzahtGd7VObdR1ABwNUA',
            }, 'ThriveFantasy': {
                '__cf_bm': 'jbc_SuDVg18rKbg7nFWKqtJAEecibV2S9p2bSZXjuDw-1725385621-1.0.1.1-J03cu6bfUVwCpT97F1cM5rCFYfMOOPBeW1Gx74FXuZfpcyjD.hxqCIK9LQMPdU8fvah4_bPNba3LCBcRnTSmJw',
            },
            'UnderdogFantasy': {
                '__cf_bm': 'lz7mfq8j3DbD1Uk5X7wgejJSrJAHV13lV7208c.nZuc-1725470254-1.0.1.1-27Cuj4s2EIyqyKwXIAbhEaax1e7mCVLNm46uA1mF3E5VpNoyXFgjQ4vKH__t8EXtB.DL6ekGs7PBf.zOCM_nnQ',
                '_cfuvid': 'rNjJXOGEnkp6jGAfQWPKDbfVxCD7XkAyqh0PF1RXU90-1725470254729-0.0.1.1-604800000',
                'cf_clearance': '5aZBgJKdqsbijrlaENVCbJaMdkLwEf8xaL38PB4cOMM-1722455195-1.0.1.1-76s6PM08.0.Slze2RsWlbcrHR..BJCbb3THnUCVjEOCm1rUUUwB2MfcCEOE5zzgGtrNph1VM8OTG2dUGGHOjyQ',
            }
        }

        return mapping.get(self.bookmaker)

    def get_params(self, name: str = 'prop_lines', var_1=None, var_2=None):
        mapping = {
            'Dabble': {
                'prop_lines': {
                    'exclude[]': [
                        'markets',
                        'selections',
                        'prices',
                    ],
                }
            }, 'MoneyLine': {
                'prop_lines': {
                    'userUUID': 'google-oauth2|111871382655879132434',
                    'apiKey': '90c0720d-f666-4bb8-8af6-48221004028c',
                }
            }, 'OddsShopper': {
                'prop_lines': {
                    'state': 'NJ',
                    'startDate': var_1,
                    'endDate': var_2,
                    'edgeSportsbooks': 'Circa,FanDuel,Pinnacle',
                }
            }, 'OwnersBox': {
                'markets': {
                    'sport': var_1,
                }, 'prop_lines': {
                    'sport': var_1,
                    'marketTypeId': var_2
                }
            }, 'Payday': {
                'leagues': {
                    'include_solo_contests': '1',
                }, 'contests': {
                    'leagues': var_1,
                    'include_solo_contests': '1',
                }
            },
            'Sleeper': {
                'prop_lines': {
                    'exclude_injury': 'false',
                }
            }

        }

        return mapping.get(self.bookmaker).get(name)

    def get_json_data(self, name: str = 'prop_lines', var=None):
        mapping = {
            'BoomFantasy': {
                'prop_lines': {
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
            }, 'Champ': {
                'prop_lines': {
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
                        "sport": var
                    }
                }

            }, 'HotStreak': {
                'leagues': {
                    'query': 'query system { system {\n        __typename\ngeneratedAt\nmaximumDeposit\nmaximumInPlayWager\nmaximumPregameWager\nmaximumReferrals\nminimumDeposit\nminimumInPlayWager\nminimumPregameWager\nminimumWithdraw\npublicBroadcastChannel\npusherAppKey\npusherCluster\nreferrerBonus\nsports {\n\n__typename\nid\nactive\ncreatedAt\ngeneratedAt\ninPlay\nleagues {\n\n__typename\nid\nalias\ncreatedAt\ngeneratedAt\nname\novertimeClock\novertimePeriods\nregulationClock\nregulationPeriods\nsportId\nupdatedAt\n\n}\nname\nupdatedAt\n\n}\n\n      } }',
                    'variables': {},
                    'operationName': 'system',
                }, 'prop_lines': {
                    'query': 'query search($query: String, $page: Int, $gameFilter: [String!], $sportFilter: [String!], $teamFilter: [String!], $positionFilter: [String!], $categoryFilter: [String!], $promotedFilter: Boolean, $participantFilter: [String!], $leagueFilter: [String!]) { search(query: $query, page: $page, gameFilter: $gameFilter, sportFilter: $sportFilter, teamFilter: $teamFilter, positionFilter: $positionFilter, categoryFilter: $categoryFilter, promotedFilter: $promotedFilter, participantFilter: $participantFilter, leagueFilter: $leagueFilter) {\n        __typename\ngeneratedAt\ncategoryFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\ngameFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\ngames {\n\n__typename\nid\n... on EsportGame {\n\n__typename\nid\nminimumNumberOfGames\nvideogameTitle\n\n}\n... on GolfGame {\n\n__typename\nid\npairings {\n\n__typename\nid\nbackNine\ncreatedAt\ngameId\ngeneratedAt\nparticipantIds\nteeTime\nupdatedAt\n\n}\ntournament {\n\n__typename\nid\nname\n\n}\n\n}\nleagueId\nopponents {\n\n__typename\nid\ndesignation\ngameId\nteam {\n\n__typename\nid\ncreatedAt\ngeneratedAt\nlogoUrl\nmarket\nname\nshortName\nupdatedAt\n\n}\n\n}\nperiod\nreplay\nscheduledAt\nstatus\n\n}\nleagueFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nmarkets {\n\n__typename\nid\ngeneratedAt\nlines\noptions\nprobabilities\n\n}\nparticipants {\n\n__typename\nid\ncategories\nopponentId\nplayer {\n\n__typename\nid\ndisplayName\nexternalId\nfirstName\nheadshotUrl\ninjuries {\n\n__typename\nid\ncomment\ncreatedAt\ndescription\ngeneratedAt\nstatus\nstatusDate\nupdatedAt\n\n}\nlastName\nnickname\nnumber\nposition\nshortDisplayName\ntraits\n\n}\nposition\n\n}\npositionFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nsportFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nteamFilters {\n\n__typename\ncount\ngeneratedAt\nkey\nmeta\nname\n\n}\nstats\ntotalCount\n\n      } }',
                    'variables': {
                        'query': '*',
                        'page': var,
                    },
                    'operationName': 'search',
                }
            }, 'Rebet': {
                'tourney_ids': {
                    'league_name': [],
                }, 'prop_lines': {
                    'tournament_id': var,
                    'game_type': 3,
                }
            }, 'ThriveFantasy': {
                'prop_lines': {
                    'half': 0,
                    'currentPage': 1,
                    'currentSize': 20,
                }
            },
            'VividPicks': {
                'prop_lines': {
                    'league': 'Multi-Sport',
                    'matchUp': False,
                }
            }
        }

        return mapping.get(self.bookmaker).get(name)

    @staticmethod
    def is_league_good(league) -> bool:
        return league in IN_SEASON_LEAGUES

    def store(self, data) -> None:
        absolute_path = os.path.abspath(f'../data_samples/{self.bookmaker.lower()}_data.json')
        with open(absolute_path, 'w') as f:
            json.dump(data, f, default=str)

        print(f'[{self.bookmaker}]: {len(data)} lines')

        # self.plc.insert_many(self.prop_lines)
