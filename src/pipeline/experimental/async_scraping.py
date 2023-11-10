"""
TODO implement
"""

import requests
from scrapy import Selector
from bs4 import BeautifulSoup
import pandas as pd
import time
import json

import sqlite3
from src.utils.exceptions import NoLinksException, UserInterruptException, AllLinksProcessedException, EmptySurveyException
from src.utils.setting_logger import Logger
from src.utils.get_config import config

import aiohttp
import asyncio

from src.pipeline.pipeline_step_abc import PipelineStepABC

logger = Logger(__name__).get_logger()


class AsyncScraper(PipelineStepABC):

    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.links = []
        self.output_table = "scraped_offers"
        # this query returns links in the current survey, that have not yet been processed. If a given link is in the scraped_offers for this survey, it won't be returned
        #self.query = f"""
        #with temp_links as (select survey_id, link, 'https://www.otodom.pl/' || link link_new from survey_links)
        #select survey_id, link, link_new from temp_links
        #where (survey_id ='{self.survey_id}') and (link_new not in (select link from scraped_offers where survey_id ='{self.survey_id}'));
        #"""
        self.query = f"select survey_id, link from survey_links where survey_id='{self.survey_id}'"


    def load_previous_step_data(self):
        super().load_previous_step_data()
        # Loading links to list
        self.links = self.df['link'].to_list()
        self.links = [f'https://www.otodom.pl/{link}' for link in self.links]


    def process(self):
        results = asyncio.run(main(self.links))
        self.df_out = pd.DataFrame(results)


async def scrape_real_estate_offer(session, url):
    html = await fetch(session, url)
    if html is None:
        return None  # Or handle this case as you see fit
        print('html is none')
    result = await extract_info_from_response(html, url)
    return result


async def fetch(session, url):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.text()
        else:
            raise ValueError(f"Failed to fetch {url}: HTTP status {response.status}")


async def extract_info_from_response(html, url):
    if not html or not isinstance(html, str):
        raise ValueError(f"No HTML content to parse for link: {url}")
    selector = Selector(text=html)
    soup = BeautifulSoup(html, 'html.parser')
    result = {}
    try:
        result['link'] = url
        result['title'] = selector.xpath("//h1[@data-cy='adPageAdTitle']/text()").get()
        result['price'] = selector.xpath("//strong[@data-cy='adPageHeaderPrice']/text()").get()
        result['adress'] = selector.xpath("//a[@aria-label='Adres']/text()").get()
        result['sq_m_price'] = selector.xpath("//div[@aria-label='Cena za metr kwadratowy']/text()").get()

        result['area'] = selector.xpath('//div[@aria-label="Powierzchnia"]/div[2]/div[1]/text()').get()
        result['ownership_type'] = selector.xpath('//div[@aria-label="Forma własności"]/div[2]/div[1]/text()').get()
        result['n_rooms'] = soup.find('div', attrs={"aria-label": "Liczba pokoi"}).find('div',attrs={"class": 'css-1wi2w6s enb64yk5'}).get_text(strip=True)
        result['state'] = selector.xpath('//div[@aria-label="Stan wykończenia"]/div[2]/div[1]/text()').get()
        result['floor'] = selector.xpath('//div[@aria-label="Piętro"]/div[2]/div[1]/text()').get()
        result['rent'] = selector.xpath('//div[@aria-label="Czynsz"]/div[2]/div[1]/text()').get()
        result['remote'] = selector.xpath('//div[@aria-label="Obsługa zdalna"]/div[2]/div[1]/text()').get()
        result['balcony'] = selector.xpath('//div[@aria-label="Balkon / ogród / taras"]/div[2]/div[1]/text()').get()
        result['heating'] = selector.xpath('//div[@aria-label="Ogrzewanie"]/div[2]/div[1]/text()').get()
        result['parking'] = selector.xpath('//div[@aria-label="Miejsce parkingowe"]/div[2]/div[1]/text()').get()

        result['market'] = selector.xpath('//div[@aria-label="Rynek"]/div[2]/div[1]/text()').get()
        result['offerent_type'] = selector.xpath('//div[@aria-label="Typ ogłoszeniodawcy"]/div[2]/div[1]/text()').get()
        result['available_from'] = selector.xpath('//div[@aria-label="Dostępne od"]/div[2]/div[1]/text()').get()
        result['year_built'] = selector.xpath('//div[@aria-label="Rok budowy"]/div[2]/div[1]/text()').get()
        result['building_type'] = selector.xpath('//div[@aria-label="Rodzaj zabudowy"]/div[2]/div[1]/text()').get()
        result['windows'] = selector.xpath('//div[@aria-label="Okna"]/div[2]/div[1]/text()').get()
        result['elevator'] = selector.xpath('//div[@aria-label="Winda"]/div[2]/div[1]/text()').get()
        result['media'] = selector.xpath('//div[@aria-label="Media"]/div[2]/div[1]/text()').get()
        result['safety'] = selector.xpath('//div[@aria-label="Zabezpieczenia"]/div[2]/div[1]/text()').get()
        result['equipment'] = selector.xpath('//div[@aria-label="Wyposażenie"]/div[2]/div[1]/text()').get()
        result['additional_info'] = selector.xpath(
            '//div[@aria-label="Informacje dodatkowe"]/div[2]/div[1]/text()').get()
        result['building_material'] = selector.xpath('//div[@aria-label="Materiał budynku"]/div[2]/div[1]/text()').get()
    except Exception as e:
        print(f"Exception occured: {e}")

    # extracting description
    description_div = soup.find('div', class_='css-1wekrze e1lbnp621')
    if description_div:
        result['description'] = description_div.get_text(strip=False)
    else:
        result['description'] = None

    return result


async def main(urls):
    results = []
    async with aiohttp.ClientSession(headers=config.headers) as session:
        tasks = [asyncio.create_task(scrape_real_estate_offer(session, url)) for url in urls]

        for task in asyncio.as_completed(tasks):
            try:
                result = await task
                # Assuming the result includes the 'link' to use as the dict key
                results.append(result)
            except Exception as e:
                print(f"Error during scraping: {e}")
                # Handle the error in a way that makes sense for your application
                # For example, you might want to log the error or append None to results

    return results

