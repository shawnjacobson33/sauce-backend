from typing import Optional

import aiohttp

from app.services.utils.requesting.maps import PAYLOAD_MAP


class Requesting:

    @staticmethod
    def get_payload(domain: str, source_name: str):
        if payload_domain := PAYLOAD_MAP.get(domain):
            if payload := payload_domain.get(source_name):
                return payload

            raise ValueError(f"Payload for {source_name} not found")
        raise ValueError(f"Payload domain {domain} not found")

    @staticmethod
    async def post(url: str, **kwargs) -> dict:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()

                raise Exception(f"Failed to post to {url} with status code {resp.status}")

    @staticmethod
    async def fetch(url: str, **kwargs) -> Optional[dict]:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()

                raise Exception(f"Failed to fetch from {url} with status code {resp.status}")
