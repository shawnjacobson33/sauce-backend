import asyncio
import requests


async def fetch(url, func, *args, **kwargs):
    response = await asyncio.to_thread(requests.get, url, **kwargs)
    if response.status_code == 200:
        await func(response.text, *args)
    else:
        print(f"Failed to retrieve {url} with status code {response.status_code}")




