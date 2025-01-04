from typing import Optional

import aiohttp


PAYLOADS = {
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
    }
}


def get_payload(source_name: str):
    if payload := PAYLOADS.get(source_name):
        return payload

    raise ValueError(f"Payload for {source_name} not found")


async def post(url: str, **kwargs) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.post(url, **kwargs) as resp:
            if resp.status == 200:
                return await resp.json()

            print(f"Failed to retrieve {url} with status code {resp.status}")
            print("ERROR: ", end='')
            async for chunk in resp.content.iter_chunked(1024):
                print(chunk.decode())


async def fetch(url: str, **kwargs) -> Optional[dict]:
    async with aiohttp.ClientSession() as session:
        async with session.get(url, **kwargs) as resp:
            if resp.status == 200:
                return await resp.json()

            print(f"Failed to retrieve {url} with status code {resp.status}")
            print("ERROR: ", end='')
            async for chunk in resp.content.iter_chunked(1024):
                print(chunk.decode())


