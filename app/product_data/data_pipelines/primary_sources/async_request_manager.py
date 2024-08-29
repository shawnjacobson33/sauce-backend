import asyncio

import cloudscraper


class AsyncRequestManager:
    def __init__(self):
        self.scraper = cloudscraper.create_scraper()

    async def get_thread(self, url, **kwargs):
        return await asyncio.to_thread(self.scraper.get, url, **kwargs)

    async def post_thread(self, url, **kwargs):
        return await asyncio.to_thread(self.scraper.post, url, **kwargs)

    async def get(self, url, func, *args, **kwargs):
        response = await self.get_thread(url, **kwargs)
        if response.status_code == 200:
            await func(response, *args)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")

    async def post(self, url, func, *args, **kwargs):
        response = await self.post_thread(url, **kwargs)
        if response.status_code == 200:
            await func(response, *args)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")
