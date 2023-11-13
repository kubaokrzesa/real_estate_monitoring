"""
This module creates Survey - entity that tracks each run of the scraping and processing pipeline,
links that will be processed in a given survey, are also determined here.
"""
import datetime
import sqlite3
from typing import Optional
import requests
from scrapy import Selector
import pandas as pd
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.special_steps.async_links_collection import async_collect_links
from src.db.sql_code import survey_table_insert

logger = Logger(__name__).get_logger()


def get_max_page_num(survey_type: str, max_page_num: int, base_link: str) -> int:
    """
    Determines the maximum number of pages for a given survey.

    If the survey type is 'test',
    it uses the provided max_page_num. Otherwise, it fetches the number of pages from a website using the base link.

    Args:
        survey_type (str): The type of the survey, e.g., 'test'.
        max_page_num (int): The maximum number of pages to consider for 'test' survey types.
        base_link (str): The base URL used for fetching the number of pages.

    Returns:
        int: The maximum number of pages to process in the survey.
    """
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
    """
    A class to create and manage surveys for collecting real estate offer data.

    Attributes:
        db (str): Database connection string.
        survey_type (str): Type of the survey (e.g., 'test', 'full', 'incremental').
        location (str): The geographical location of the survey.
        max_page_num (Optional[int]): The maximum number of pages to scrape. (works only for 'test' survey type)
        base_link (str): Base URL for scraping.
        survey_date (str): Date when the survey is created.
        n_pages (int): Number of pages to scrape.
        survey_number (int): Number identifying the survey, if multiple for given location, type and time exist
        survey_id (str): A unique identifier for the survey.
        links (List[str]): List of offer links for the survey.
        df_out (Optional[pd.DataFrame]): DataFrame holding the output data.
        use_async_links_scraping (bool): if asynchronous scraping for link collection should be used.

    Methods:
        get_survey_number(): Retrieves the survey number based on existing surveys.
        create_survey_in_table(): Creates a new survey record in the database.
        collect_links_list(): Collects links to real estate offers.
        clean_and_save_links_to_db(): Cleans and saves the collected links to the database.
    """
    def __init__(self, db: str, survey_type: str, location: str, max_page_num: Optional[int] = None):
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
        self.use_async_links_scraping = config.use_async_links_scraping
        logger.info(f"Survey id generated: {self.survey_id}")

    def get_survey_number(self, survey_date: str, survey_type: str, location: str) -> int:
        """
        Retrieves the survey number based on the survey date, type, and location.

        Args:
            survey_date (str): The date of the survey.
            survey_type (str): The type of the survey.
            location (str): The location of the survey.

        Returns:
            int: survey number.
        """
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
        """
        Creates a new entry in the survey table in the database for the current survey.
        """
        logger.info(f"Creating survey in database")
        db_entry = (self.survey_id, self.survey_date, self.survey_type, self.location, self.n_pages)

        with sqlite3.connect(self.db) as conn:
            cur = conn.cursor()
            # insert records to db
            cur.execute(survey_table_insert, db_entry)
            conn.commit()
        logger.info(f"Survey added successfully")

    def collect_links_list(self):
        """
        Collects links to real estate offers to be processed in following steps.
        """
        logger.info(f"Collecting links to real estate offers")
        if self.use_async_links_scraping:
            logger.info(f"Using async to collect links from pages")
            self.links = async_collect_links(self.n_pages)
        else:
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
        """
       Removes duplicate and already processed links and saves the unique links to the database.
       """
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







