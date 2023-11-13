"""
Collecting links for a survey using asyncio
"""
from src.utils.get_config import config
from src.utils.setting_logger import Logger
import aiohttp
import asyncio
from scrapy import Selector
from typing import List, Optional

logger = Logger(__name__).get_logger()


async def fetch(session: aiohttp.ClientSession, url: str) -> str:
    """
    Asynchronously fetches the HTML content of a given URL using aiohttp.

    Args:
        session (aiohttp.ClientSession): The HTTP session to use for the request.
        url (str): The URL to fetch.

    Returns:
        str: The HTML content of the page.

    Raises:
        ValueError: If the request to the URL fails.
    """
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()
        else:
            raise ValueError(f"Failed to fetch {url}: HTTP status {response.status}")


def get_links_list_from_response(response: str) -> List[str]:
    """
    Extracts a list of links from the HTML response using XPath.

    Args:
        response (str): The HTML content of a webpage.

    Returns:
        List[str]: A list of extracted links.
    """
    selector = Selector(text=response)
    listings = selector.xpath("//a[@data-cy='listing-item-link']/@href")
    links_list = [listing.get() for listing in listings]
    return links_list


async def scrape_offer_links(session: aiohttp.ClientSession, url: str) -> Optional[List[str]]:
    """
    Asynchronously scrapes offer links from a given URL.

    Args:
        session (aiohttp.ClientSession): The HTTP session to use for the request.
        url (str): The URL to scrape for offer links.

    Returns:
        Optional[List[str]]: A list of offer links if found, otherwise None.
    """
    html = await fetch(session, url)
    if html is None:
        return None
        logger.info('html is none')
    result = get_links_list_from_response(html)
    return result


async def main(urls: List[str]) -> List[str]:
    """
    Asynchronously processes a list of URLs to scrape offer links.

    Args:
        urls (List[str]): A list of URLs to scrape.

    Returns:
        List[str]: A consolidated list of all scraped offer links.
    """
    results = []
    async with aiohttp.ClientSession(headers=config.headers) as session:
        tasks = [asyncio.create_task(scrape_offer_links(session, url)) for url in urls]

        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                results.extend(result)
            except Exception as e:
                logger.info(f"Error during scraping: {e}")

    return results


def async_collect_links(n_pages: int) -> List[str]:
    """
    Collects offer links from multiple pages asynchronously.

    Args:
        n_pages (int): Number of pages to scrape.

    Returns:
        List[str]: A list of all collected offer links.
    """
    urls = map(lambda x: config.base_link.format(x), range(1, n_pages))
    results = asyncio.run(main(urls))
    return results
