import asyncio

import aiohttp
from cloudscraper import CloudScraper, CloudflareLoopProtection

from in_dev.async_cloudscraper.async_cloudflare import AsyncCloudFlare


REDIRECT_CODES = (
    301,  # moved
    302,  # found
    303,  # other
    307,  # temporary redirect
    308,  # permanent redirect
)


class AsyncCloudScraper(CloudScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    async def perform_request(self, method, url, *args, **kwargs) -> aiohttp.ClientResponse:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, *args, **kwargs) as resp:
                return resp

    @staticmethod
    def _is_redirect(resp: aiohttp.ClientResponse):
        return 'location' in resp.headers and resp.status in REDIRECT_CODES

    async def request(self, method, url, *args, **kwargs):
        # pylint: disable=E0203
        if kwargs.get('proxies') and kwargs.get('proxies') != self.proxies:
            self.proxies = kwargs.get('proxies')

        # ------------------------------------------------------------------------------- #
        # Pre-Hook the request via user defined function.
        # ------------------------------------------------------------------------------- #

        if self.requestPreHook:
            (method, url, args, kwargs) = self.requestPreHook(
                self,
                method,
                url,
                *args,
                **kwargs
            )

        # ------------------------------------------------------------------------------- #
        # Make the request via aiohttp.
        # ------------------------------------------------------------------------------- #

        response = self.perform_request(method, url, *args, **kwargs)

        # ------------------------------------------------------------------------------- #
        # Debug the request via the Response object.
        # ------------------------------------------------------------------------------- #

        if self.debug:
            self.debugRequest(response)

        # ------------------------------------------------------------------------------- #
        # Post-Hook the request aka Post-Hook the response via user defined function.
        # ------------------------------------------------------------------------------- #

        if self.requestPostHook:
            if response := self.requestPostHook(self, response):
                if self.debug:
                    print('==== requestPostHook Debug ====')
                    self.debugRequest(response)

        # ------------------------------------------------------------------------------- #
        resp_text = await response.text()
        if not self.disableCloudflareV1:
            cloudflare_v1 = AsyncCloudFlare(self)

            # ------------------------------------------------------------------------------- #
            # Check if Cloudflare v1 anti-bot is on
            # ------------------------------------------------------------------------------- #

            if cloudflare_v1.is_Challenge_Request(response):
                # ------------------------------------------------------------------------------- #
                # Try to solve the challenge and send it back
                # ------------------------------------------------------------------------------- #

                if self._solveDepthCnt >= self.solveDepth:
                    _ = self._solveDepthCnt
                    self.simpleException(
                        CloudflareLoopProtection,
                        f"!!Loop Protection!! We have tried to solve {_} time(s) in a row."
                    )

                self._solveDepthCnt += 1

                resp_text = await cloudflare_v1.challenge_response(response, **kwargs)
            else:
                # CHANGED
                if not self._is_redirect(response) and response.status not in [429, 503]:
                    self._solveDepthCnt = 0

        return resp_text

    async def get(self, url: str, *args, **kwargs):
        return await self.request('GET', url, *args, **kwargs)

    async def post(self, url: str, *args, **kwargs):
        return await self.request('POST', url, *args, **kwargs)


async def main():
    scraper = AsyncCloudScraper()
    response_text = await scraper.get('https://scrapfly.io/blog/web-scraping-speed/')
    asd = 213

if __name__ == '__main__':
    asyncio.run(main())