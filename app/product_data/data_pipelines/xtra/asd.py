import time

import requests

headers = {
    'Host': 'kube-prod.chalkboard.io',
    'x-firebase-appcheck': 'eyJraWQiOiJRNmZ5eEEiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxOjM2MjkyMTAyNTQwOmlvczo5ZWJmZWZkM2ExZTEyMjI4N2IyZWE5IiwiYXVkIjpbInByb2plY3RzXC8zNjI5MjEwMjU0MCIsInByb2plY3RzXC90YWlsZC0yNjIzMDUiXSwicHJvdmlkZXIiOiJkZXZpY2VfY2hlY2tfYXBwX2F0dGVzdCIsImlzcyI6Imh0dHBzOlwvXC9maXJlYmFzZWFwcGNoZWNrLmdvb2dsZWFwaXMuY29tXC8zNjI5MjEwMjU0MCIsImV4cCI6MTcyNTMyNjYyOSwiaWF0IjoxNzI1MzI0ODI5LCJqdGkiOiI3X3ZyR1U5TlI1UEIzMWNDSGU5X3FqRTVvRi1GdEFQWFFhc3ZYZ2VWcmF3In0.cFU3qc_-oNA3pi_MAVVYp2X04icoXOMLntHUM7P-YOwfLDtb7UHWK3y-p_24BycbRZ6K57nH9dD_bSndA4gADIBU2jgwyoiPraQyhw6yayPi3p-BQAuumfzEat7XuWZwf4LVvnXf6LLuetWL5JJ11Sk9im3oQ4CMEmGQrjEevCuswWUxO0ISrLIO4pPTxeDY3W7vw53WLoAyH3HqqbIbNEMvsJLle1CLceowD1JNYk_3n8yLT2-qpE28CwYl93c5swNynQC9dYbPsz63ie1JoIcbVEFbygNLZ_tZ244EiptnVOb-q1G6DE6zF-yc_-M520Gr1g1uAS6E9khKEf6Tch4Wh-zEJqHy8Qe8IITFD04NJI40TES0kB1zaevefIBh0PQ280IksShxwVk_QGCd4e13ib7F2nljPelO4AZ0AhM_cXMNFv0xWeQzQNOuKBDrkf6RYDpRl9ztvJn4HNwvAWgWT8UO7S7GLTjfb5_IeqiqfT6bjsjbcyBAY4gIGmiO',
    'clientos': 'ios',
    'generated': '2024-09-03T00:53:48',
    'x-datadog-parent-id': '8865947239838336815',
    'user-agent': 'Chalkboard/5804 CFNetwork/1498.700.2 Darwin/23.6.0',
    'x-datadog-trace-id': '1496898285171192130',
    'buildnumber': '5804',
    'x-datadog-origin': 'rum',
    'signature': 'ecce9a8015605013f5285f0fec1c820cef4f3322',
    'baggage': 'sentry-environment=production,sentry-public_key=d1e0cacfd20f28096511384f84af1ee8,sentry-trace_id=481b0a3de8834a5d8b1bab121b266ae2',
    'deviceid': '2FC0D25E-5DED-44F0-8608-4A41DD306773',
    'devicemodel': 'iPhone 14 Pro',
    'x-datadog-sampling-priority': '0',
    'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6ImNjNWU0MTg0M2M1ZDUyZTY4ZWY1M2UyYmVjOTgxNDNkYTE0NDkwNWUiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vdGFpbGQtMjYyMzA1IiwiYXVkIjoidGFpbGQtMjYyMzA1IiwiYXV0aF90aW1lIjoxNzIzOTMxNzk4LCJ1c2VyX2lkIjoiaWE3N1B6bzRYdk1MeFVYczFJNkRLQ2drVEVvMiIsInN1YiI6ImlhNzdQem80WHZNTHhVWHMxSTZES0Nna1RFbzIiLCJpYXQiOjE3MjUzMjI5NzcsImV4cCI6MTcyNTMyNjU3NywiZW1haWwiOiJ0aGVyZWFsc2xpbV90ZW1wQGNoYWxrYm9hcmQuaW8iLCJlbWFpbF92ZXJpZmllZCI6ZmFsc2UsImZpcmViYXNlIjp7ImlkZW50aXRpZXMiOnsiZW1haWwiOlsidGhlcmVhbHNsaW1fdGVtcEBjaGFsa2JvYXJkLmlvIl19LCJzaWduX2luX3Byb3ZpZGVyIjoicGFzc3dvcmQifX0.hr1uMawMR-kEutXseED_1vUIlJGOI2U0jOahES8SWFKCqqAdomLnpNWHm0tGDFsaypsu7ZC1cHUk-qJTUhTNrCiJJVwZS6TV37pK69jjMepUBRjJu4y80KHOJL4blHI6z9lz87clSC6pGLdYG-pUghiyBRxBW0P01BaAj3wrAU1V2wWuIAy2Pij7V2UZCvwvQpBBqd5VAlNuAVmfkeT-UQTWQfTVmOKhhZG37AFklA-NWjoyKgK-U17BqhUyckF1wiVKQ8E2xSG-r5ehS60tmlUEerbtZ272wVna-GDRgANSyNOvnq8mGMM1y7Tw4nZksUAa_DcPnczfGwm-Y_8sjg',
    'tracestate': 'dd=s:0;o:rum;p:7b0a2c0a73eb472f',
    'timezone': 'America/Chicago',
    'accept-language': 'en-US,en;q=0.9',
    'accept': 'application/json, text/plain, */*',
    'if-none-match': 'W/"a8-quAGkHFDTcgyhiOdu2l0Wc3O/js"',
    'traceparent': '00-000000000000000014c60d0f38853142-7b0a2c0a73eb472f-00',
    'currentversion': '1.0.58',
    'sentry-trace': '481b0a3de8834a5d8b1bab121b266ae2-8987308e3f2ede87',
    'key': 'beta',
}

# params = {
#     'page': '1',
# }
params = [
    'props', 'prop-bets', 'player-props', 'lines', 'betting-lines', 'odds', 'games', 'matches', 'fixtures', 'nfl',
    'mlb', 'leagues', 'players', 'player-stats', 'athletes', 'bets', 'bet-slips', 'wagers', 'sports', 'football',
    'basketball', 'markets', 'betting-markets', 'sports-markets', 'contests', 'tournaments', 'competitions',
    'stats', 'statistics', 'dfs', 'daily-fantasy', 'fantasy', 'live-odds', 'odds-updates', 'scores', 'results',
    'game-results', 'predictions', 'forecasts', 'season', 'current-season'

]

for param_1 in params:
    for param_2 in params:
        time.sleep(1)
        response = requests.get(f'https://kube-prod.chalkboard.io/base/api/{param_1}/{param_2}', headers=headers)
        print(response.status_code)
        print(f'https://kube-prod.chalkboard.io/base/api/{param_1}/{param_2}')
        print(response.content)
