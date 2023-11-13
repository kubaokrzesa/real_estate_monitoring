import pandas as pd

from src.pipeline.scraping.scraping_funcs import extract_info_from_response
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from typing import List, Dict, Optional, Any
import aiohttp
import asyncio

from src.pipeline.scraping.base_scraper import BaseScraper

logger = Logger(__name__).get_logger()


class AsyncScraper(BaseScraper):
    """
    Asynchronous scraper for extracting information from real estate offer links.

    This class extends BaseScraper to asynchronously process a list of URLs by scraping
    information from each URL and compiling the results into a DataFrame.

    Note: it can return 403 http errors if too many links are provided

    Methods:
        process(): Asynchronously scrapes data from links and stores results in a DataFrame.
        execute_step(): Orchestrates the loading, processing, and uploading of scraped data.
    """

    def process(self):
        """
        Asynchronously processes the list of links by scraping information from each link.
        Compiles the scraped data into a DataFrame.
        """
        results = asyncio.run(main(self.links))
        self.df_out = pd.DataFrame(results)

    def execute_step(self):
        """
        Executes the scraping step by loading links, processing them asynchronously, and uploading results.
        """
        self.load_previous_step_data()
        self.process()
        self.upload_results_to_db()


async def scrape_real_estate_offer(session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
    """
    Asynchronously scrapes a real estate offer from the given URL.

    Args:
        session (aiohttp.ClientSession): The HTTP session to use for the request.
        url (str): The URL of the real estate offer to scrape.

    Returns:
        Optional[Dict[str, Any]]: A dictionary containing extracted information, or None if failed.
    """
    html = await fetch(session, url)
    if html is None:
        return None
        logger.info('html is none')
    result = extract_info_from_response(html, url)
    return result


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


async def main(urls: List[str]) -> List[Dict[str, Any]]:
    """
    Asynchronously scrapes real estate offers from a list of URLs.

    Args:
        urls (List[str]): A list of URLs to scrape.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing extracted information from each URL.
    """
    results = []
    async with aiohttp.ClientSession(headers=config.headers) as session:
        tasks = [asyncio.create_task(scrape_real_estate_offer(session, url)) for url in urls]

        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                results.append(result)
            except Exception as e:
                print(f"Error during scraping: {e}")
    return results


