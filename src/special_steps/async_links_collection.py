from src.utils.get_config import config
import aiohttp
import asyncio
from scrapy import Selector


async def fetch(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()
        else:
            raise ValueError(f"Failed to fetch {url}: HTTP status {response.status}")


def get_links_list_from_response(response):
    selector = Selector(text=response)
    listings = selector.xpath("//a[@data-cy='listing-item-link']/@href")
    links_list = [listing.get() for listing in listings]
    return links_list


async def scrape_offer_links(session, url):
    html = await fetch(session, url)
    if html is None:
        return None
        print('html is none')
    result = get_links_list_from_response(html)
    return result


async def main(urls):
    results = []
    async with aiohttp.ClientSession(headers=config.headers) as session:
        tasks = [asyncio.create_task(scrape_offer_links(session, url)) for url in urls]

        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                results.extend(result)
            except Exception as e:
                print(f"Error during scraping: {e}")

    return results


def async_collect_links(n_pages):
    urls = map(lambda x: config.base_link.format(x), range(1, n_pages))
    results = asyncio.run(main(urls))
    return results
