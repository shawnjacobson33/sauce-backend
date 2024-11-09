import aiohttp


async def fetch(url, func):
    # start up a session to manage connections effectively
    async with aiohttp.ClientSession() as session:
        # make the request to the inputted url
        async with session.get(url) as response:
            # return the content
            return await func(await response.text())
