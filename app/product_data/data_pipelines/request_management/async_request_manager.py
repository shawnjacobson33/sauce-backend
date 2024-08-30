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

    async def post_bf(self, url, func, file_path: str, refresh_token: str, access_token: str, json_data):
        def get_headers(token):
            return {
                "Host": "production-boom-dfs-backend-api.boomfantasy.com",
                "content-type": "application/json",
                "access-control-allow-origin": "*",
                "accept": "application/json, text/plain, */*",
                "x-product-id": "sports_predictor",
                "authorization": f"Bearer {token}",
                "x-app-name": "Predictor",
                "accept-language": "en-US,en;q=0.9",
                "user-agent": "Predictor/1 CFNetwork/1496.0.7 Darwin/23.5.0",
                "x-device-id": "9498D138-1139-42BA-81A1-2E25990EA696",
                "x-app-build": "1",
                "x-app-version": "785",
                "x-platform": "ios"
            }

        async def parse_tokens(token_response):
            data = token_response.json()

            with open(file_path, 'w') as file:
                file.write(access_token + '\n'), file.write(refresh_token + '\n')

            return data.get('accessToken'), data.get('refreshToken')

        response = await self.post_thread(url, headers=get_headers(access_token), json=json_data)
        if response.status_code == 200:
            await func(response)
        else:
            print(f"Failed to retrieve {url} with status code {response.status_code}")
            print("Attempting to refresh tokens...")

            tokens_url = 'https://production-api.boomfantasy.com/api/v1/sessions'
            tokens_headers, tokens_json_data = {
                'Host': 'production-api.boomfantasy.com',
                'content-type': 'application/json',
                'accept': 'application/json',
                'x-product-id': 'sports_predictor',
                'authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2ZXIiOjIsInR5cCI6ImFub24iLCJ0aWQiOiJwcmVkaWN0b3IiLCJwaWQiOiJzcG9ydHNfcHJlZGljdG9yIiwiaWF0IjoxNjg3OTY1MjQwfQ.sKySbA5uKlb3TO1i9k1wlQSdhJeQfpB0H1G3VkqRRIA1IrJyxcwwB74PmP1r16_Zs00tP1QXYV2toBFLZDadaxxiI3XI9_n0r6K8DW4O71iEUz3lNZtYx3b890nNQySYG7RLLAQ4vpGEyktjxE9xSD4TE0jvTJa-nibp6s1d8Ncm7RNNb5Xkz0ugvp06wWcBAa9rZPuaMTlP4DtKdKewjFC6B4AlESdLRaEBYg8tNviyGJS4mcV8iEt3Zbd5B7XRMwUU90IvxbQKF1HSUGQW0qHIZEXgiw_HUxHs-9V4jM77r2hj0nwAIr6YlMweiNhZ09vMtTUcT6dWDEKDWyXGkMpZrzGlTuT9y5jCNNoNxmzrFwu4sFjKc4MXXu4A5vqWwe4KjbAZ7U9_f6m1DhdvsdPhgUaoqA19CUd0sVRcJCpccvdlA3qPcuP332tiVkZ8Pi2h4PyF2gS1WIwqfYAGb4VYmxgMqxFhmCKpk2oWDAB0908KkfvqAvtYxbH1s3uZORy0duZyWkQ7I7EnqsHnBayX6-SdhUocYrQJsUAWnQYcoJ9vOKwcV6je8lnQcMFaBOfHcbTmd9Vp92wEwPVsn1UtKlkhJHAy_bzMEzPPBH2e8fGNpkiSbF7Rkz5r5X5imV1PMI8ZuXmfQT_WJjmxp4RtaB2JEOXM8xSPSM4IDYg',
                'x-app-name': 'Predictor',
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Predictor/1 CFNetwork/1496.0.7 Darwin/23.5.0',
                'x-device-id': '9498D138-1139-42BA-81A1-2E25990EA696',
                'x-app-build': '1',
                'x-app-version': '785',
                'x-platform': 'ios',
            }, {
                'authentication': {
                    'type': 'refresh',
                    'credentials': {
                        'refreshToken': refresh_token,
                        'accessToken': access_token,
                    },
                },
                'eventInfo': {},
            }

            tokens_response = await self.post_thread(tokens_url, headers=tokens_headers, json=tokens_json_data)
            if tokens_response.status_code == 200:
                access_token, refresh_token = await parse_tokens(tokens_response)

                response = await self.post_thread(url, headers=get_headers(access_token), json=json_data)
                if response.status_code == 200:
                    await func(response)
                else:
                    print(f"Failed to retrieve {url} with status code {response.status_code}")
            else:
                print(f"Failed to retrieve {tokens_url} with status code {tokens_response.status_code}")
