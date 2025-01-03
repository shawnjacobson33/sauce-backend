import asyncio
import re
import time
from copy import deepcopy
from urllib.parse import urlparse

import aiohttp
from cloudscraper import Cloudflare
from cloudscraper.exceptions import CloudflareCaptchaProvider, CloudflareIUAMError, CloudflareSolveError


class AsyncCloudFlare(Cloudflare):
    def __init__(self, cloudscraper):
        super().__init__(cloudscraper)

    @staticmethod
    def is_captcha_challenge(resp: aiohttp.ClientResponse, resp_text: str):
        try:
            return (
                    resp.headers.get('Server', '').startswith('cloudflare')
                    and resp.status == 403
                    and re.search(r'/cdn-cgi/images/trace/(captcha|managed)/', resp_text, re.M | re.S)
                    and re.search(
                r'''<form .*?="challenge-form" action="/\S+__cf_chl_f_tk=''',
                resp_text,
                re.M | re.S
            )
            )
        except AttributeError:
            pass

        return False

    async def challenge_response(self, resp: aiohttp.ClientResponse, **kwargs):
        resp_text = await resp.text()
        if self.is_captcha_challenge(resp, resp_text):
            # ------------------------------------------------------------------------------- #
            # double down on the request as some websites are only checking
            # if cfuid is populated before issuing Captcha.
            # ------------------------------------------------------------------------------- #

            if self.cloudscraper.doubleDown:
                # CHANGED
                resp = self.cloudscraper.perform_request(resp.request_info.method, resp.url, **kwargs)

            if not self.is_captcha_challenge(resp, resp_text):
                return resp_text

            # ------------------------------------------------------------------------------- #
            # if no captcha provider raise a runtime error.
            # ------------------------------------------------------------------------------- #

            if (
                not self.cloudscraper.captcha
                or not isinstance(self.cloudscraper.captcha, dict)
                or not self.cloudscraper.captcha.get('provider')
            ):
                self.cloudscraper.simpleException(
                    CloudflareCaptchaProvider,
                    "Cloudflare Captcha detected, unfortunately you haven't loaded an anti Captcha provider "
                    "correctly via the 'captcha' parameter."
                )

            # ------------------------------------------------------------------------------- #
            # if provider is return_response, return the response without doing anything.
            # ------------------------------------------------------------------------------- #

            if self.cloudscraper.captcha.get('provider') == 'return_response':
                return resp_text

            # ------------------------------------------------------------------------------- #
            # Submit request to parser wrapper to solve captcha
            # ------------------------------------------------------------------------------- #

            submit_url = self.captcha_Challenge_Response(
                self.cloudscraper.captcha.get('provider'),
                self.cloudscraper.captcha,
                resp_text,
                resp.url
            )
        else:
            # ------------------------------------------------------------------------------- #
            # Cloudflare requires a delay before solving the challenge
            # ------------------------------------------------------------------------------- #

            if not self.cloudscraper.delay:
                try:
                    delay = float(
                        re.search(
                            r'submit\(\);\r?\n\s*},\s*([0-9]+)',
                            resp_text
                        ).group(1)
                    ) / float(1000)
                    if isinstance(delay, (int, float)):
                        self.cloudscraper.delay = delay
                except (AttributeError, ValueError):
                    self.cloudscraper.simpleException(
                        CloudflareIUAMError,
                        "Cloudflare IUAM possibility malformed, issue extracing delay value."
                    )

            # CHANGED
            await asyncio.sleep(self.cloudscraper.delay)

            # ------------------------------------------------------------------------------- #

            submit_url = self.IUAM_Challenge_Response(
                resp_text,
                resp.url,
                self.cloudscraper.interpreter
            )

        # ------------------------------------------------------------------------------- #
        # Send the Challenge Response back to Cloudflare
        # ------------------------------------------------------------------------------- #

        if submit_url:

            def update_attr(obj, name, new_value):
                try:
                    obj[name].update(new_value)
                    return obj[name]
                except (AttributeError, KeyError):
                    obj[name] = {}
                    obj[name].update(new_value)
                    return obj[name]

            cloudflare_kwargs = deepcopy(kwargs)
            cloudflare_kwargs['allow_redirects'] = False
            cloudflare_kwargs['data'] = update_attr(
                cloudflare_kwargs,
                'data',
                submit_url['data']
            )


            # CHANGED
            cloudflare_kwargs['headers'] = update_attr(
                cloudflare_kwargs,
                'headers',
                {
                    'Origin': f'{resp.url.scheme}://{resp.url.host}:{resp.url.port}',
                    'Referer': str(resp.url)
                }
            )

            # CHANGED
            challenge_submit_response = await self.cloudscraper.request(
                'POST',
                submit_url['url'],
                **cloudflare_kwargs
            )

            if challenge_submit_response.status == 400:
                self.cloudscraper.simpleException(
                    CloudflareSolveError,
                    'Invalid challenge answer detected, Cloudflare broken?'
                )

            # ------------------------------------------------------------------------------- #
            # Return response if Cloudflare is doing content pass through instead of 3xx
            # else request with redirect URL also handle protocol scheme change http -> https
            # ------------------------------------------------------------------------------- #

            # CHANGED
            if not challenge_submit_response.is_redirect:
                return await challenge_submit_response.text()

            else:
                cloudflare_kwargs = deepcopy(kwargs)
                # CHANGED
                cloudflare_kwargs['headers'] = update_attr(
                    cloudflare_kwargs,
                    'headers',
                    {'Referer': str(challenge_submit_response.url)}
                )

                if not urlparse(challenge_submit_response.headers['Location']).netloc:
                    redirect_location = urljoin(
                        challenge_submit_response.url,
                        challenge_submit_response.headers['Location']
                    )
                else:
                    redirect_location = challenge_submit_response.headers['Location']

                return self.cloudscraper.request(
                    resp.request.method,
                    redirect_location,
                    **cloudflare_kwargs
                )

        # ------------------------------------------------------------------------------- #
        # We shouldn't be here...
        # Re-request the original query and/or process again....
        # ------------------------------------------------------------------------------- #

        return self.cloudscraper.request(resp.request.method, resp.url, **kwargs)