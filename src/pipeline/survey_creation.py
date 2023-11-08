import datetime
import sqlite3

import requests
from scrapy import Selector
import pandas as pd
from src.utils.setting_logger import Logger
from src.utils.get_config import config

from src.pipeline.sql_code import survey_table_insert

logger = Logger(__name__).get_logger()


def get_max_page_num(survey_type, max_page_num, base_link):
    if survey_type == 'test' and max_page_num is not None:
        n_pages = max_page_num
    else:
        url = base_link.format(str(1))
        response = requests.get(url, headers=config.headers)
        selector = Selector(response)
        n_pages = selector.xpath("//nav[@data-cy='pagination']/a[3][@aria-label]//text()").get()
        n_pages = int(n_pages)
    logger.info(f"Found {str(n_pages)} pages")
    return n_pages


class SurveyCreator:

    def __init__(self, db, survey_type, location, max_page_num=None):
        logger.info("Creating new survey")
        self.base_link = config.base_link
        self.db = db
        self.survey_date = datetime.datetime.today().strftime('%Y-%m-%d')
        self.survey_type = survey_type
        self.location = location
        self.n_pages = get_max_page_num(survey_type, max_page_num, self.base_link)
        self.survey_number = self.get_survey_number(self.survey_date, self.survey_type, self.location)
        self.survey_id = f"{self.survey_type}_{self.survey_date}_{self.location}_{str(self.survey_number)}"
        self.links = []
        self.df_out = None
        logger.info(f"Survey id generated: {self.survey_id}")

    def get_survey_number(self, survey_date, survey_type, location):
        with sqlite3.connect(self.db) as conn:
                try:
                    d = pd.read_sql_query(
                        f"SELECT survey_id FROM surveys where survey_date='{survey_date}' and survey_type='{survey_type}' and location='{location}';",
                                          conn)
                    similar_surveys_already = len(d)
                except TypeError:
                    similar_surveys_already = 0
        survey_number = similar_surveys_already + 1
        return survey_number

    def create_survey_in_table(self):
        logger.info(f"Creating survey in database")
        db_entry = (self.survey_id, self.survey_date, self.survey_type, self.location, self.n_pages)

        with sqlite3.connect(self.db) as conn:
            cur = conn.cursor()
            # insert records to db
            cur.execute(survey_table_insert, db_entry)
            conn.commit()
        logger.info(f"Survey added successfully")

    def collect_links_list(self):
        logger.info(f"Collecting links to real estate offers")
        for page_num in range(self.n_pages):
            url = self.base_link.format(str(page_num))
            logger.info(f"Visiting page with offers number {str(page_num)}, url: {url}")

            response = requests.get(url, headers=config.headers)
            selector = Selector(response)

            listings = selector.xpath("//a[@data-cy='listing-item-link']/@href")

            links_list = [listing.get() for listing in listings]
            self.links.extend(links_list)
        logger.info(f"Total number of links collected: {str(len(self.links))}")

    def clean_and_save_links_to_db(self):
        logger.info(f"Removing duplicated links")
        links_unique = list(set(self.links))
        if self.survey_type == 'incremental':
            logger.info(f"Incremental survey, removing links already present in scraped_offers")
            with sqlite3.connect(self.db) as conn:
                d = pd.read_sql_query("select distinct link from scraped_offers", conn)
            d = d['link'].apply(lambda x: x.replace('https://www.otodom.pl/', ''))

            links_unique = [item for item in links_unique if item not in d.values]
        logger.info(f"Number of links after processing: {str(len(links_unique))}")
        self.df_out = pd.DataFrame({'survey_id': [self.survey_id] * len(links_unique), 'link': links_unique})
        logger.info(f"Uploading links to database")
        with sqlite3.connect(self.db) as conn:
            self.df_out.to_sql('survey_links', conn, if_exists='append', index=False)
        logger.info(f"Links uploaded successfully")







