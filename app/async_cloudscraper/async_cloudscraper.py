import logging

import aiohttp
import requests
from aiohttp import ClientResponse
from cloudscraper import CloudScraper, Cloudflare, CloudflareLoopProtection


class AsyncCloudScraper(CloudScraper):
    def __init__(self, *args, **kwargs):
        super().__init__(args, kwargs)

    async def perform_request(self, method, url, *args, **kwargs):
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, *args, **kwargs) as resp:
                return await resp.text()

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
        # Make the request via requests.
        # ------------------------------------------------------------------------------- #

        response = await self.perform_request(method, url, *args, **kwargs)

        # ------------------------------------------------------------------------------- #
        # Debug the request via the Response object.
        # ------------------------------------------------------------------------------- #

        if self.debug:
            self.debugRequest(response)

        # ------------------------------------------------------------------------------- #
        # Post-Hook the request aka Post-Hook the response via user defined function.
        # ------------------------------------------------------------------------------- #

        if self.requestPostHook:
            if response := self.requestPostHook(self, response):  # Give me walrus in 3.7!!!
                if self.debug:
                    print('==== requestPostHook Debug ====')
                    self.debugRequest(response)

        # ------------------------------------------------------------------------------- #

        if not self.disableCloudflareV1:
            cloudflareV1 = Cloudflare(self)

            # ------------------------------------------------------------------------------- #
            # Check if Cloudflare v1 anti-bot is on
            # ------------------------------------------------------------------------------- #

            if cloudflareV1.is_Challenge_Request(response):
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

                response = cloudflareV1.Challenge_Response(response, **kwargs)
            else:
                if not response.is_redirect and response.status_code not in [429, 503]:
                    self._solveDepthCnt = 0

        return response

    async def get(self, url: str, *args, **kwargs):
        return await self.request('GET', url, *args, **kwargs)

    async def post(self, url: str, *args, **kwargs):
        return await self.request('POST', url, *args, **kwargs)
