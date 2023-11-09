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

from src.pipeline.pipeline_step_abc import PipelineStepABC

logger = Logger(__name__).get_logger()


class Scraper(PipelineStepABC):

    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.links = []
        self.output_table = "scraped_offers"
        # this query returns links in the current survey, that have not yet been processed. If a given link is in the scraped_offers for this survey, it won't be returned
        self.query = f"""
        with temp_links as (select survey_id, link, 'https://www.otodom.pl/' || link link_new from survey_links)
        select survey_id, link, link_new from temp_links
        where (survey_id ='{self.survey_id}') and (link_new not in (select link from scraped_offers where survey_id ='{self.survey_id}'));
        """

    def execute_step(self):
        self.load_previous_step_data()
        self.process()

    def load_previous_step_data(self):
        super().load_previous_step_data()

        # Validity checks
        with sqlite3.connect(self.db) as conn:
            of_cnt = pd.read_sql_query(f"select count(*) of_cnt from scraped_offers where survey_id = '{self.survey_id}'", conn)
            l_cnt = pd.read_sql_query(f"select count(*) l_cnt from survey_links where survey_id = '{self.survey_id}'", conn)
        of_cnt = of_cnt['of_cnt'].iloc[0]
        l_cnt = l_cnt['l_cnt'].iloc[0]
        left_to_process = l_cnt - of_cnt
        logger.info(f"{left_to_process}/{l_cnt} links left to process in survey {self.survey_id}")
        if l_cnt == 0:
            raise EmptySurveyException(f"Survey {self.survey_id} has no corresponding links in the links table")
        if left_to_process == 0:
            raise AllLinksProcessedException(f"All links in {self.survey_id} have already been processed and saved to database")

        # Loading links to list
        self.links = self.df['link'].to_list()

    def process(self):
        logger.info(f"Iterating links to extract information")
        if not self.links:
            raise NoLinksException("The list of links is empty, fill it before iterating")

        res_ls = []
        try:
            while self.links:
                link = self.links.pop()
                logger.info(f"Analysing link: {link}")
                try:
                    extracted_info = _extract_info_from_link(link)
                    res_ls.append(extracted_info)
                except Exception as e:
                    logger.info(f"Exception occurred: {str(e)}, retrying")
                    for i in range(1, 11):
                        logger.info(f"Pausing for {str(i)} seconds")
                        time.sleep(i)
                        try:
                            extracted_info = _extract_info_from_link(link)
                            res_ls.append(extracted_info)
                            break
                        finally:
                            pass
                    if i == 10:
                        logger.info(f"Maximum retries reached, breaking")
                        break
        except KeyboardInterrupt:
            logger.info("User interrupted the process. Exiting...")
            raise UserInterruptException
        finally:
            self.df_out = pd.DataFrame(res_ls)
            self.df_out['survey_id'] = self.survey_id
            self.upload_results_to_db()


def _extract_info_from_link(link):
    link_full = 'https://www.otodom.pl/' + link

    logger.info(f"analyzing {link_full}")

    response = requests.get(link_full, headers=config.headers)
    selector = Selector(response)
    soup = BeautifulSoup(response.content, 'html.parser')

    result = {}

    result['link'] = link_full
    result['title'] = selector.xpath("//h1[@data-cy='adPageAdTitle']/text()").get()
    result['price'] = selector.xpath("//strong[@data-cy='adPageHeaderPrice']/text()").get()
    result['adress'] = selector.xpath("//a[@aria-label='Adres']/text()").get()
    result['sq_m_price'] = selector.xpath("//div[@aria-label='Cena za metr kwadratowy']/text()").get()

    result['area'] = selector.xpath('//div[@aria-label="Powierzchnia"]/div[2]/div[1]/text()').get()
    result['ownership_type'] = selector.xpath('//div[@aria-label="Forma własności"]/div[2]/div[1]/text()').get()
    #result['n_rooms'] = selector.xpath('//div[@aria-label="Liczba pokoi"]/div[2]/div[1]/text()').get()
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

    # extracting description
    description_div = soup.find('div', class_='css-1wekrze e1lbnp621')
    if description_div:
        result['description'] = description_div.get_text(strip=False)
    else:
        result['description'] = None

    return result
