import json
import os
import time
import uuid
from datetime import datetime

from bs4 import BeautifulSoup
import asyncio

from app.product_data.data_pipelines.request_management import AsyncRequestManager
from pymongo import MongoClient
from pymongo.collection import Collection


class DraftKingsPick6:
    def __init__(self, batch_id: uuid.UUID, arm: AsyncRequestManager, msc: Collection):
        self.prop_lines = []
        self.batch_id = batch_id

        self.arm = arm
        self.msc = msc
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            # 'cookie': '_gcl_au=1.1.237242021.1722660083; _tgpc=6a677fed-e15e-59f8-8254-5307dbb48445; _scid=2b1e8c43-c965-4a84-ac6b-b501dc855fb5; __ssid=274b5d1daab02c43d897ca5ef189e37; _fbp=fb.1.1722660083167.741600682836015041; _svsid=27b49d34efe00c10df5f5fbf8e61c022; site=US-DK; ASP.NET_SessionId=tihhtzs4hgcnq1grhfmtcj22; VIDN=46476035361; SN=1223548523; LID=1; SINFN=PID=&AOID=&PUID=0&SSEG=&GLI=0&LID=1&site=US-DK; _csrf=45cccf01-0bd4-40d3-a842-ee57fd0bc160; notice_behavior=implied,us; _tt_enable_cookie=1; _ttp=IEG4E7twvEapgZziXkYFcvbkSAJ; _dpm_id.16f4=8618284c-d63a-489f-96e6-0923568503c1.1722660083.2.1723012752.1722660083.d51ce44f-87bd-424e-a9d7-48d4db861060; _sp_srt_id.16f4=f0f089b0-474e-4f86-937f-31dd1a4808b1.1722660083.2.1723012753.1722660083.6337e216-ef90-470c-bb7f-ffeb4124be04.b2deda4e-67c4-4926-aedc-1627e0a7bee8...0; _hjSessionUser_2150570=eyJpZCI6IjE3YzQ0NTBmLTUxMjUtNTgwOC05ZDBiLTBiMThjMjU4MjVkMyIsImNyZWF0ZWQiOjE3MjMwMTI3MzYyOTksImV4aXN0aW5nIjp0cnVlfQ==; ak_bmsc=C786F5291F64349D09A196F3FD39B481~000000000000000000000000000000~YAAQjdpHaJObwkyRAQAAnCpGTxgWxdQfaHvXJ1YVdHocq7skIsH3vV6LbAfB0r9Rk5D0dTWYXuhKxnQnH6iRAZaSnVuOVQhU2e8rss6FesE89P3TFB+HnR906GxWmwVG5ll/7jBa595Ow8pDOZlc4zOmboPWZSOfsY3vRfuNgy4MXUEhbIXH4Y33V/3cOE0Tsd18y/ZkD1xg3VWGw9034JIBFgvAzHz7qetSnHLz4VIZRdjWTOLJA0TDI6qfr/vC/HAX51TjklkboNIhM300J48nv2/ZmZx9f4fEFB/z1YLLU3oUDT00aCpp4D2p6W09I5MWGDwO3aG8OvDAZNwP+lFNp1KALzK2cnK6tX4yJS9vU2MQJJ+HZrV4vmtrU8v2JMfbXZWYPGrFhC4WIp5/gyy1WJ2QwqAWftbyulLcB5m1/ITf0oAzsg==; CH-time-zone=America%2FChicago; notice_behavior=implied,us; _rdt_uuid=1722660082914.cebfd368-329c-42d7-9c77-a9cde85a636d; _gid=GA1.2.741583253.1723612205; _tglksd=eyJzIjoiODNmYzJiYjQtNDkzNy01YTQ4LWEyZDUtY2MzZWY2MTIyYjlhIiwic3QiOjE3MjM2MTIyMDUzMzIsInNvZCI6IihkaXJlY3QpIiwic29kdCI6MTcyMzAxMjcyMTQ1Nywic29kcyI6Im8iLCJzb2RzdCI6MTcyMzYxMjIwNTMzMn0=; _ga_M8T3LWXCC5=GS1.2.1723612205.2.0.1723612205.60.0.0; _ga_MCSJH508Q0=GS1.2.1723612205.2.0.1723612205.60.0.0; _sctr=1%7C1723611600000; _ga=GA1.1.1456577494.1722660083; STIDN=eyJDIjoxMjIzNTQ4NTIzLCJTIjo3MTkxMjQzNDQ5NiwiU1MiOjc1NjU3OTE3OTI1LCJWIjo0NjQzNjY4NzQzNywiTCI6MSwiRSI6IjIwMjQtMDgtMTRUMDU6NDM6NDcuODY4MzI4NFoiLCJTRSI6IlVTLURLIiwiVUEiOiJ1UFh5eW9waThpVkRReFVCRkkvWlhvcGRxMlhkak0yRzhWSzdMODNYZ1AwPSIsIkRLIjoiYTQ1ZDJjYTYtZjFkMC00NTY3LTgxYzEtODUxMzk1MjU3MTY1IiwiREkiOiI5YzZlNDllNi1lZTYzLTQzOGUtYjMwYS03MTBhYmJmZjEyM2MiLCJERCI6NDUwMzk4MzYyMTR9; STH=25e76a39b847926c8ad01e96a73be33615d1c7de5c79ede3ac320e5d7c288e5e; PRV=3P=1&V=46436687437&E=1723698827; _abck=B5B34FBDD0D8C251BC0B18CBD8FBB58F~0~YAAQXdpHaK9qoS6RAQAAPCB9TwzDiohDagYbII4+gBqyNgsRgqYpsPtEVhPeO+HYQd6MbMDaoyBvP5LG2FDBkDcKnhb6QSZWOOwXnxBPInsXSRuSLcz93dILkENbu4YsSbShgtw417Zv9yj4MUrSYVAXuz9020KDWiyhRm2uBzWF3KW17rlP9MRnugrF3wsiTuF10STNsB8qC58w731oPiazf04ELJ7hoLnAQLKxYtTg22Lw/yp6PSYQTFmPH/le6yzDKYKQH73GdDhjAp9/MGo+ENe4i7nn3U3oINWa/TmXKsPBr7Qi3mYbHCNrgAkQs/zUYVW8GkBndZTkbU2sOTfZAunpat77XANHQlG6yFuqWJU0FZcwXh9Wz6+CQEHBQTcQm9sDsHYDZYFZ7t0BPVA9QoLWEKK+mATgCYY3xenfJP7Hk1sAxe5wxPDrUu9XFW+MnthLMEdPTM/MoL3YZwBgQ7ekpvvZA3z7nvM6ZPRWGZ/g6ZxRRaTs+80+IO1OGIaHgMvjFO8n~-1~-1~1723619086; TAsessionID=bf4ac304-6abc-46a3-ae4c-337215a48628|NEW; GC-HTML5-UUID=5C97C38A-23C8-4D5B-B511-81B40C6DD1BA; hgg=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2aWQiOiI0NjQzNjY4NzQzNyIsImRraC0xMjYiOiI4MzNUZF9ZSiIsImRrZS0xMjYiOiIwIiwiZGtlLTIwNCI6IjcxMCIsImRrZS0yODgiOiIxMTI4IiwiZGtlLTMxOCI6IjEyNjAiLCJka2UtMzQ1IjoiMTM1MyIsImRrZS0zNDYiOiIxMzU2IiwiZGtlLTQyOSI6IjE3MDUiLCJka2UtNzAwIjoiMjk5MiIsImRrZS03MzkiOiIzMTQwIiwiZGtlLTc1NyI6IjMyMTIiLCJka2gtNzY4IjoicVNjQ0VjcWkiLCJka2UtNzY4IjoiMCIsImRrZS04MDYiOiIzNDI2IiwiZGtlLTgwNyI6IjM0MzciLCJka2UtODI0IjoiMzUxMSIsImRrZS04MjUiOiIzNTE0IiwiZGtlLTgzNiI6IjM1NzAiLCJka2gtODk1IjoiOGVTdlpEbzAiLCJka2UtODk1IjoiMCIsImRrZS05MDMiOiIzODQ4IiwiZGtlLTkxNyI6IjM5MTMiLCJka2UtOTQ3IjoiNDA0MiIsImRrZS05NzYiOiI0MTcxIiwiZGtlLTEyNzciOiI1NDExIiwiZGtlLTEzMjgiOiI1NjUzIiwiZGtlLTE1NjEiOiI2NzMzIiwiZGtoLTE2NDEiOiJSMGtfbG1rRyIsImRrZS0xNjQxIjoiMCIsImRrZS0xNjUzIjoiNzEzMSIsImRrZS0xNjU2IjoiNzE1MSIsImRrZS0xNjg2IjoiNzI3MSIsImRrZS0xNjg5IjoiNzI4NyIsImRrZS0xNjk1IjoiNzMyOSIsImRrZS0xNzA5IjoiNzM4MyIsImRrZS0xNzU0IjoiNzYwNSIsImRrZS0xNzYwIjoiNzY0OSIsImRrZS0xNzY2IjoiNzY3NSIsImRrZS0xNzcwIjoiNzY5MiIsImRraC0xNzc0IjoiMlNjcGtNYXUiLCJka2UtMTc3NCI6IjAiLCJka2UtMTc4MCI6Ijc3MzEiLCJka2UtMTc5NCI6Ijc4MDEiLCJka2UtMTgwMSI6Ijc4MzkiLCJka2gtMTgwNSI6Ik9Ha2Jsa0h4IiwiZGtlLTE4MDUiOiIwIiwiZGtlLTE4MjgiOiI3OTU2IiwiZGtlLTE4NDEiOiI4MDI1IiwiZGtlLTE4NTEiOiI4MDk3IiwiZGtlLTE4NjEiOiI4MTU3IiwiZGtlLTE4NjQiOiI4MTczIiwiZGtlLTE4NjgiOiI4MTg4IiwiZGtoLTE4NzkiOiJ2YjlZaXpsTiIsImRrZS0xODc5IjoiMCIsImRrZS0xODgwIjoiODIzMiIsImRrZS0xODgzIjoiODI0MiIsImRrZS0xODkwIjoiODI3NyIsImRrZS0xODk1IjoiODMwMCIsImRrZS0xOTAxIjoiODMyNCIsImRrZS0xODIyIjoiNzkyMyIsImRrZS0xODk4IjoiODMxMyIsImRraC0xOTA0IjoiMEN3eEVGMWoiLCJka2UtMTkwNCI6IjAiLCJka2gtMTkwNSI6ImRmQkdfWHg5IiwiZGtlLTE5MDUiOiIwIiwiZGtlLTE5MDkiOiI4MzU4IiwiZGtoLTE5MTIiOiJLbTkwOWxfdCIsImRrZS0xOTEyIjoiMCIsImRraC0xOTEzIjoiaTJ6eENRdzQiLCJka2UtMTkxMyI6IjAiLCJka2gtMTkxNiI6IlMzdjM1ZmZYIiwiZGtlLTE5MTYiOiIwIiwiZGtlLTE5MzgiOiI4NDU3IiwiZGtoLTE5NDUiOiJyQ3RXT2g4ZiIsImRrZS0xOTQ1IjoiMCIsImRraC0xOTQ5IjoiWEpDc0pvYmQiLCJka2UtMTk0OSI6IjAiLCJka2gtMTk1MCI6IkRYY2pGYlZKIiwiZGtlLTE5NTAiOiIwIiwiZGtoLTE5NTEiOiJKa0JXczVlViIsImRrZS0xOTUxIjoiMCIsImRraC0xOTUyIjoiTnY0WEd6WWEiLCJka2UtMTk1MiI6IjAiLCJka2gtMTk1MyI6IkhROFVYVDRGIiwiZGtlLTE5NTMiOiIwIiwiZGtoLTE5NTQiOiJvazgwcTlZRCIsImRrZS0xOTU0IjoiMCIsImRraC0xOTU1IjoiZjBXVEdUX0UiLCJka2UtMTk1NSI6IjAiLCJka2UtMTk1NiI6Ijg1MjkiLCJka2UtMTk2NyI6Ijg1NzYiLCJka2gtMTkxOCI6ImhqOFZuX3Z1IiwiZGtlLTE5MTgiOiIwIiwibmJmIjoxNzIzNjE1NjA0LCJleHAiOjE3MjM2MTU5MDQsImlhdCI6MTcyMzYxNTYwNCwiaXNzIjoiZGsifQ.73nW8ONBX49HgsrZXGQ5gqxWXpI0NzBZpUFNe7vIhDA; ss-id=zFHrcjFCaAFrW54DDXkI; ss-pid=UFmOZna7iZqUTKrhtjXt; bm_sz=28448B22E1D907148798CCD20A957764~YAAQiHFAF1IwhUmRAQAAZS+BTxi25RLg3H+pr3BF7ZSpP5LKwdT+S6IumSIh3Eh24X/dCd6iOlj58aJ/1mKrEkF0LkgBt8tUrumelvMTGIcuF60MKnRnFPSfy40iMWMfj+FxTrbMF+I5FinaKZOp9vylbdQekAb0LhftG1KcporG1Z6GLH9ZXrwijsJ+FFEpHXzBbJTaOpg/mLlOE+CQUR7YqVVEt/4gn/ui3C959D7GI8k57SyPK0TLmSwOmlhUYg0yJAs/Wj46lfGv0t7du3XihpgWHp+qKXMIVmSLnmiZk+/SW+LSO8ekA2g2S6s2LKiHqZ0kWFEofGDQrWhTjG+xLZFVVC80CfPB3zWYGVBKsc3clCc/RVeL7iomScZp71YdZdQkYEfmXraRppvLuxG+WY9wDoyBQOLCuQusadIHZSseMKfk1kib9ArsI2kpdgwW12jLQkJ/iJpUHEIDIgjpO/40BQMiKAsaa4yUu2N9S9hDtFH6gA3aYQ2yw1/h53Xx3usdrW4BNIMvypayj3QTH8aU7wQRqYmfyO2+mew1r1No8KBZWiXEhzZtffw0ke0=~3158068~3356210; _root-session-prod=eyJ1c2VyS2V5IjoiIn0%3D.cf%2BrgnhUId0pWDHJIE%2FDeq8fmS7mTB8RRRFcm02gZMM; _scid_r=2b1e8c43-c965-4a84-ac6b-b501dc855fb5; _sc_cspv=https%3A%2F%2Ftr.snapchat.com%2Fconfig%2Fcom%2Fe844d917-f415-4a90-9631-beffe27b0d29.js%3Fv%3D3.25.1-2408082241; STE="2024-08-14T06:39:15.6076038Z"; bm_sv=BFDEF0A9A72A0A75DA930DC6CD9F79BE~YAAQiHFAF+oyhUmRAQAAoVCBTximjhpzwtbyi6OMw9V7Sy8cp8ZoTgITk9nO/FLzqGnrC1SKl3Mn9/qcechBhvyg2l1mc2k2J1CChFnXjc9dWxzscwow/UJ6LrW92MwKBHxhtmYoR3MeiLdyV7523T5bNQc8/Hj57jCq/ha5e5TIONxIiQ4sQq3SF0DBQHQm3WioyJLQb/cERpMap8tvaMFRoi1+ba/4UTL+weEn5ch23ifd3tnz9ClTUHiW4gWuPzWY+df1~1; _uetsid=bd4bd3e059fa11ef8e5fbbae8b6ac9be; _uetvid=0c4c86a0999311eea12683206d9e4d59; ab.storage.sessionId.3bf0f7f8-c059-46d9-b743-275aeb2997ed=%7B%22g%22%3A%22ce16afe6-5ba6-acfb-0177-772967a2167a%22%2C%22e%22%3A1723617563622%2C%22c%22%3A1723612535374%2C%22l%22%3A1723615763622%7D; _ga_QG8WHJSQMJ=GS1.1.1723615570.5.1.1723615764.2.0.0',
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

    async def start(self):
        url = "https://pick6.draftkings.com/"

        await self.arm.get(url, self._parse_sports, headers=self.headers)

    async def _parse_sports(self, response):
        tasks, soup = [], BeautifulSoup(response.text, 'html.parser')
        for league in [sport_div.text for sport_div in soup.find_all('div', {'class': 'dkcss-7q5fzm'}) if
                       not sport_div.text.isnumeric()]:
            url = response.url + f"?sport={league}&_data=routes%2F_index"

            tasks.append(self.arm.get(url, self._parse_lines, league, headers=self.headers))

        await asyncio.gather(*tasks)

        relative_path = 'data_samples/draftkingspick6_data.json'
        absolute_path = os.path.abspath(relative_path)
        with open(absolute_path, 'w') as f:
            json.dump(self.prop_lines, f, default=str)

        print(f'[DraftKingsPick6]: {len(self.prop_lines)} lines')

    async def _parse_lines(self, response, league):
        text = response.text

        # needs cleaning
        def clean_json():
            first_index = text.find('data:{')
            return text[:first_index]

        data = json.loads(clean_json()).get('pickableIdToPickableMap')

        for pick in data.values():
            market_id, market, subject, subject_team, game_time, position, line = '', '', '', '', '', '', ''
            pickable, active_market = pick.get('pickable'), pick.get('activeMarket')
            if pickable:
                market_category = pickable.get('marketCategory')
                if market_category:
                    market = market_category.get('marketName')
                    market_id = self.msc.find_one({'DraftKings Pick6': market}, {'_id': 1})
                    if market_id:
                        market_id = market_id.get('_id')
                for entity in pickable.get('pickableEntities', []):
                    subject, competitions = entity.get('displayName'), entity.get('pickableCompetitions')
                    if competitions:
                        first_competition = competitions[0]
                        position, team_data = first_competition.get('positionName'), first_competition.get('team')
                        if team_data:
                            subject_team = team_data.get('abbreviation')

                        summary = first_competition.get('competitionSummary')
                        if summary:
                            game_time = summary.get('startTime')

            if active_market:
                line = active_market.get('targetValue')

            for label in ['Over', 'Under']:
                self.prop_lines.append({
                    'batch_id': self.batch_id,
                    'time_processed': datetime.now(),
                    'league': league,
                    'game_time': game_time,
                    'market_category': 'player_props',
                    'market_id': market_id,
                    'market_name': market,
                    'subject_team': subject_team,
                    'subject': subject,
                    'position': position,
                    'bookmaker': "DraftKings Pick6",
                    'label': label,
                    'line': line
                })


async def main():
    client = MongoClient('mongodb://localhost:27017/')

    db = client['sauce']

    spider = DraftKingsPick6(batch_id=uuid.uuid4(), arm=AsyncRequestManager(), msc=db['markets'])
    start_time = time.time()
    await spider.start()
    end_time = time.time()

    print(f'[DraftKingsPick6]: {round(end_time - start_time, 2)}s')


if __name__ == "__main__":
    asyncio.run(main())
