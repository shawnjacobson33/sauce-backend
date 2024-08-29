import asyncio
from datetime import datetime, timedelta
import json
from urllib.parse import urlencode
import cloudscraper
import pandas as pd


class OddsShopperSpider:
    def __init__(self, batch_id: str):
        self.prop_lines = []
        self.batch_id = batch_id
        self.scraper = cloudscraper.create_scraper()

    async def start_requests(self):
        url = f'https://www.oddsshopper.com/api/processingInfo/all'
        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'priority': 'u=1, i',
            'referer': 'https://www.oddsshopper.com/liveodds/mlb',
            'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
        }
        cookies = {
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
        }

        response = await self.async_get(url, headers=headers, cookies=cookies)
        if response.status_code == 200:
            await self.parse_processing_info(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def async_get(self, url, **kwargs):
        return await asyncio.to_thread(self.scraper.get, url, **kwargs)

    async def parse_processing_info(self, response):
        data = response.json()

        # Now proceed to fetch the matchups using the lastProcessed time
        url = f'https://www.oddsshopper.com/api/liveOdds/offers?league=all'

        response = await self.async_get(url)
        if response.status_code == 200:
            await self.parse_matchups(response, data.get('lastProcessed'))
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def parse_matchups(self, response, last_processed):
        data = response.json()

        tasks = []
        for offer_category in data.get('offerCategories', []):
            if offer_category.get('name') not in {'GameProps', 'PlayerFutures', 'TeamFutures'}:
                market_category = offer_category.get('name')
                for offer in offer_category.get('offers', []):
                    offer_id, now = offer.get('id'), datetime.utcnow()
                    # Calculate current datetime and future date
                    start_date = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
                    end_date = (now + timedelta(days=8)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

                    params = {
                        'state': 'NJ',
                        'startDate': start_date,
                        'endDate': end_date,
                        'edgeSportsbooks': 'Circa,FanDuel,Pinnacle',
                    }
                    url = f"https://api.oddsshopper.com/api/offers/{offer_id}/outcomes/live?{urlencode(params)}"
                    headers = {
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

                    tasks.append(self.fetch_and_parse_lines(url, headers, params, last_processed, offer.get('leagueCode'), market_category))

        await asyncio.gather(*tasks)

        self.count_lines_per_bookmaker()
        with open("oddsshopper_data.json", "w") as f:
            json.dump(self.prop_lines, f, default=str)

        print(len(self.prop_lines))

    async def fetch_and_parse_lines(self, url, headers, params, last_processed, league, market_category):
        response = await self.async_get(url, headers=headers, params=params)
        if response.status_code == 200:
            await self.parse_lines(response, last_processed, league, market_category)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def parse_lines(self, response, last_processed, league, market_category):
        data = response.json()

        for event in data:
            game_time, market, subject = event.get('startDate'), event.get('offerName'), event.get('eventName')
            for side in event.get('sides', []):
                label = side.get('label', subject)
                for outcome in side.get('outcomes', []):
                    prop_line = {
                        'batch_id': self.batch_id,
                        'time_processed': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'last_updated': last_processed,
                        'league': league,
                        'game_time': game_time,
                        'market_category': market_category,
                        'market': market,
                        'subject': outcome.get('label', subject),
                        'bookmaker': outcome.get('sportsbookCode'),
                        'label': label,
                        'line': outcome.get('line', '0.5'),
                        'odds': outcome.get('odds'),
                        'oddsshopper_ev': round(outcome.get('ev', float('nan')), 3)
                    }

                    self.prop_lines.append(prop_line)

    def count_lines_per_bookmaker(self):
        df = pd.DataFrame(self.prop_lines)

        print(df.groupby('bookmaker').size().reset_index())


async def main():
    spider = OddsShopperSpider(batch_id='123')
    await spider.start_requests()

if __name__ == "__main__":
    asyncio.run(main())
