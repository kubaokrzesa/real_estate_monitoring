import pandas as pd

from src.pipeline.scraping.scraping_funcs import extract_info_from_response
from src.utils.setting_logger import Logger
from src.utils.get_config import config

import aiohttp
import asyncio

from src.pipeline.scraping.base_scraper import BaseScraper

logger = Logger(__name__).get_logger()


class AsyncScraper(BaseScraper):

    def process(self):
        results = asyncio.run(main(self.links))
        self.df_out = pd.DataFrame(results)

    def execute_step(self):
        self.load_previous_step_data()
        self.process()
        self.upload_results_to_db()


async def scrape_real_estate_offer(session, url):
    html = await fetch(session, url)
    if html is None:
        return None
        print('html is none')
    result = extract_info_from_response(html, url)
    return result


async def fetch(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()
        else:
            raise ValueError(f"Failed to fetch {url}: HTTP status {response.status}")


async def main(urls):
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

