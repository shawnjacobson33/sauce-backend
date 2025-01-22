from typing import Optional

import aiohttp

from pipelines.utils.exceptions import RequestingError
from pipelines.utils.requesting.maps import PAYLOAD_MAP


class Requesting:

    @staticmethod
    def get_payload(source_name: str, domain: str = None) -> dict | None:
        if payload := PAYLOAD_MAP.get(source_name):
            if domain:
                if payload_domain_specific := payload.get(domain):
                    return payload_domain_specific

                raise ValueError(f"Payload for {source_name} and domain {domain} not found")

            return payload

        raise ValueError(f"Payload for {source_name} not found")

    @staticmethod
    async def post(url: str, to_html: bool = False, **kwargs) -> dict:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, **kwargs) as resp:
                    if resp.status == 200:
                        return await resp.json() if not to_html else await resp.text()

        except Exception as e:
            raise RequestingError(f"Failed to fetch from {url} with status code {resp.status}", e)

    @staticmethod
    async def fetch(url: str, to_html: bool = False, **kwargs) -> Optional[dict]:
        # Todo: These are temporary solutions while the aio-cloudscraper is being built.
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, **kwargs) as resp:
                    if resp.status == 200:
                        return await resp.json() if not to_html else await resp.text()

        except Exception as e:
            raise RequestingError(f"Failed to fetch from {url} with status code {resp.status}", e)