import asyncio
import cloudscraper
import requests
import os


# TODO: LOG THESE MESSAGES INSTEAD OF PRINT


def read_tokens() -> tuple[str, str, str]:
    # get the working directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # get the absolute file path where the tokens are stored
    absolute_path = os.path.join(script_dir, 'tokens', 'boomfantasy_tokens.txt')
    # open the file in read mode
    with open(absolute_path, 'r') as file:
        # extract the access token and refresh token
        access_token, refresh_token = [line.strip() for line in file.readlines()[:2]]

    # return the extracted data in a dictionary
    return absolute_path, access_token, refresh_token


async def parse_tokens(token_response, file_path: str, access_token: str, refresh_token: str):
    data = token_response.json()

    with open(file_path, 'w') as file:
        file.write(access_token + '\n'), file.write(refresh_token + '\n')

    return data.get('accessToken'), data.get('refreshToken')


class RequestManager:
    def __init__(self, use_requests: bool = False):
        self.use_requests = use_requests
        self.scraper = cloudscraper.create_scraper()

    async def get_thread(self, url, **kwargs):
        request_func = self.scraper.get if not self.use_requests else requests.get
        return await asyncio.to_thread(request_func, url, **kwargs)

    async def post_thread(self, url, **kwargs):
        request_func = self.scraper.post if not self.use_requests else requests.post
        return await asyncio.to_thread(request_func, url, **kwargs)

    async def get(self, url, func, *args, **kwargs):
        response = await self.get_thread(url, **kwargs)
        if response.status_code == 200:
            await func(response, *args)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")
            print(f"ERROR: {response.content}")

    async def post(self, url, func, *args, **kwargs):
        response = await self.post_thread(url, **kwargs)
        if response.status_code == 200:
            await func(response, *args)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")
            print(f"ERROR: {response.content}")

    async def get_bf(self, url: str, tokens_data: dict, func, headers, params) -> None:
        # get file_path to tokens and the current tokens
        file_path, access_token, refresh_token = read_tokens()
        # update the header's authorization field with the access token
        headers['authorization'] = f'Bearer {access_token}'
        # make the request on a separate thread with structured headers and params
        response = await self.get_thread(url, headers=headers, params=params)
        # if it is a successful request then execute function
        if response.status_code == 200:
            # asynchronously call the function
            await func(response)

        # unsuccessful request
        else:
            # output a message saying that the request was unsuccessful because the tokens were stale
            print(f"Failed to retrieve {url} with status code {response.status_code}")
            print("Attempting to refresh tokens...")
            # update the json data's access token field
            tokens_data['json_data']['authentication']['credentials']['accessToken'] = access_token
            # update the json data's refresh token field
            tokens_data['json_data']['authentication']['credentials']['refreshToken'] = refresh_token
            # make a request for the fresh tokens
            tokens_response = await self.post_thread(tokens_data['url'], headers=tokens_data['headers'], json=tokens_data['json_data'])
            # if it is a successful request then execute
            if tokens_response.status_code == 200:
                # parse the tokens from the response
                access_token, refresh_token = await parse_tokens(tokens_response, file_path, access_token, refresh_token)
                # update the header's authorization field with the access token
                headers['authorization'] = f'Bearer {access_token}'
                # make another request using the fresh tokens for prop lines
                response = await self.get_thread(url, headers=headers, params=params)
                # if request is successful keep executing
                if response.status_code == 200:
                    # asynchronously call the function
                    await func(response)

                # unsuccessful request
                else:
                    # output a message with details
                    print(f"Failed to retrieve {url} with status code {response.status_code}")
            else:
                # output a message for failure with tokens request
                print(f"Failed to retrieve {tokens_data['url']} with status code {tokens_response.status_code}")
