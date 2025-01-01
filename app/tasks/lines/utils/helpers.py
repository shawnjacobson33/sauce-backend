from typing import Optional

import aiohttp


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
