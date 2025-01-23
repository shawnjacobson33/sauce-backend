from typing import Optional

import aiohttp

from pipelines.utils.exceptions import RequestingError
from pipelines.utils.requesting.maps import PAYLOAD_MAP


class Requesting:
    """
    A utility class for handling HTTP requests and payload retrieval.

    Methods:
        get_payload(source_name: str, domain: str = None) -> dict | None:
            Retrieves the payload for the given source name and optional domain.

        post(url: str, to_html: bool = False, **kwargs) -> dict:
            Sends a POST request to the specified URL and returns the response.

        fetch(url: str, to_html: bool = False, **kwargs) -> Optional[dict]:
            Sends a GET request to the specified URL and returns the response.
    """

    @staticmethod
    def get_payload(source_name: str, domain: str = None) -> dict | None:
        """
        Retrieves the payload for the given source name and optional domain.

        Args:
            source_name (str): The name of the source.
            domain (str, optional): The domain of the source. Defaults to None.

        Returns:
            dict | None: The payload for the source and domain, or None if not found.

        Raises:
            ValueError: If the payload for the source or domain is not found.
        """
        if payload := PAYLOAD_MAP.get(source_name):
            if domain:
                if payload_domain_specific := payload.get(domain):
                    return payload_domain_specific

                raise ValueError(f"Payload for {source_name} and domain {domain} not found")

            return payload

        raise ValueError(f"Payload for {source_name} not found")

    @staticmethod
    async def post(url: str, to_html: bool = False, **kwargs) -> dict:
        """
        Sends a POST request to the specified URL and returns the response.

        Args:
            url (str): The URL to send the POST request to.
            to_html (bool, optional): Whether to return the response as HTML. Defaults to False.
            **kwargs: Additional arguments to pass to the request.

        Returns:
            dict: The response from the POST request.

        Raises:
            RequestingError: If the request fails.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, **kwargs) as resp:
                    if resp.status == 200:
                        return await resp.json() if not to_html else await resp.text()

        except Exception as e:
            raise RequestingError(f"Failed to fetch from {url} with status code {resp.status}", e)

    @staticmethod
    async def fetch(url: str, to_html: bool = False, **kwargs) -> Optional[dict]:
        """
        Sends a GET request to the specified URL and returns the response.

        Args:
            url (str): The URL to send the GET request to.
            to_html (bool, optional): Whether to return the response as HTML. Defaults to False.
            **kwargs: Additional arguments to pass to the request.

        Returns:
            Optional[dict]: The response from the GET request, or None if the request fails.

        Raises:
            RequestingError: If the request fails.
        """
        # Todo: These are temporary solutions while the aio-cloudscraper is being built.
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, **kwargs) as resp:
                    if resp.status == 200:
                        return await resp.json() if not to_html else await resp.text()

        except Exception as e:
            raise RequestingError(f"Failed to fetch from {url} with status code {resp.status}", e)