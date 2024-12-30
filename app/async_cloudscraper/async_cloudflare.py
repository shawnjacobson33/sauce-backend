import asyncio
import re
import time
from copy import deepcopy
from urllib.parse import urlparse

from cloudscraper import Cloudflare
from cloudscraper.exceptions import CloudflareCaptchaProvider, CloudflareIUAMError, CloudflareSolveError


class AsyncCloudFlare(Cloudflare):
    def __init__(self, cloudscraper):
        super().__init__(cloudscraper)

    async def Challenge_Response(self, resp, **kwargs):
        if self.is_Captcha_Challenge(resp):
            # ------------------------------------------------------------------------------- #
            # double down on the request as some websites are only checking
            # if cfuid is populated before issuing Captcha.
            # ------------------------------------------------------------------------------- #

            if self.cloudscraper.doubleDown:
                # CHANGED
                resp = await self.cloudscraper.perform_request(resp.request.method, resp.url, **kwargs)

            if not self.is_Captcha_Challenge(resp):
                return resp

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
                return resp

            # ------------------------------------------------------------------------------- #
            # Submit request to parser wrapper to solve captcha
            # ------------------------------------------------------------------------------- #

            submit_url = self.captcha_Challenge_Response(
                self.cloudscraper.captcha.get('provider'),
                self.cloudscraper.captcha,
                resp.text,
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
                            resp.text
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
                resp.text,
                resp.url,
                self.cloudscraper.interpreter
            )

        # ------------------------------------------------------------------------------- #
        # Send the Challenge Response back to Cloudflare
        # ------------------------------------------------------------------------------- #

        if submit_url:

            def updateAttr(obj, name, newValue):
                try:
                    obj[name].update(newValue)
                    return obj[name]
                except (AttributeError, KeyError):
                    obj[name] = {}
                    obj[name].update(newValue)
                    return obj[name]

            cloudflare_kwargs = deepcopy(kwargs)
            cloudflare_kwargs['allow_redirects'] = False
            cloudflare_kwargs['data'] = updateAttr(
                cloudflare_kwargs,
                'data',
                submit_url['data']
            )

            urlParsed = urlparse(resp.url)
            cloudflare_kwargs['headers'] = updateAttr(
                cloudflare_kwargs,
                'headers',
                {
                    'Origin': f'{urlParsed.scheme}://{urlParsed.netloc}',
                    'Referer': resp.url
                }
            )

            # CHANGED
            challengeSubmitResponse = await self.cloudscraper.request(
                'POST',
                submit_url['url'],
                **cloudflare_kwargs
            )

            if challengeSubmitResponse.status_code == 400:
                self.cloudscraper.simpleException(
                    CloudflareSolveError,
                    'Invalid challenge answer detected, Cloudflare broken?'
                )

            # ------------------------------------------------------------------------------- #
            # Return response if Cloudflare is doing content pass through instead of 3xx
            # else request with redirect URL also handle protocol scheme change http -> https
            # ------------------------------------------------------------------------------- #

            if not challengeSubmitResponse.is_redirect:
                return challengeSubmitResponse

            else:
                cloudflare_kwargs = deepcopy(kwargs)
                cloudflare_kwargs['headers'] = updateAttr(
                    cloudflare_kwargs,
                    'headers',
                    {'Referer': challengeSubmitResponse.url}
                )

                if not urlparse(challengeSubmitResponse.headers['Location']).netloc:
                    redirect_location = urljoin(
                        challengeSubmitResponse.url,
                        challengeSubmitResponse.headers['Location']
                    )
                else:
                    redirect_location = challengeSubmitResponse.headers['Location']

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