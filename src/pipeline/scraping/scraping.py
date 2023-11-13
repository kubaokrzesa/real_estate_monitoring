import requests
import pandas as pd
import time

from src.utils.exceptions import NoLinksException, UserInterruptException
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.scraping.scraping_funcs import extract_info_from_response
from typing import List, Dict, Any

from src.pipeline.scraping.base_scraper import BaseScraper

logger = Logger(__name__).get_logger()


class Scraper(BaseScraper):
    """
    Scraper class for extracting information from a list of web links.

    It processes links one by one, which is slower but more reliable.

    This class extends BaseScraper to process a list of links by extracting information
    from each link and storing the results.

    Methods:
        execute_step(): Orchestrates the loading and processing of links.
        process(): Iterates over links, extracts information, and handles retries and exceptions.
    """

    def execute_step(self):
        """
        Executes the scraping step by loading links and processing them.
        """
        self.load_previous_step_data()
        self.process()

    def process(self):
        """
        Processes each link by extracting information and handling retries on failure.
        Raises NoLinksException if the list of links is empty.
        """
        logger.info(f"Iterating links to extract information")
        if not self.links:
            raise NoLinksException("The list of links is empty, fill it before iterating")

        res_ls = []
        try:
            while self.links:
                link = self.links.pop()
                logger.info(f"Analysing link: {link}")
                try:
                    html = fetch(link)
                    extracted_info = extract_info_from_response(html, link)
                    res_ls.append(extracted_info)
                except Exception as e:
                    logger.info(f"Exception occurred: {str(e)}, retrying")
                    for i in range(1, 11):
                        logger.info(f"Pausing for {str(i)} seconds")
                        time.sleep(i)
                        try:
                            html = fetch(link)
                            extracted_info = extract_info_from_response(html, link)
                            res_ls.append(extracted_info)
                            break
                        except:
                            continue
                        else:
                            break
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


def fetch(link: str) -> str:
    """
    Fetches the HTML content of a given link.

    Args:
        link (str): The URL to fetch.

    Returns:
        str: The HTML content of the page.

    Raises:
        requests.HTTPError: If the request to the URL fails.
    """
    logger.info(f"analyzing {link}")
    response = requests.get(link, headers=config.headers)
    response.raise_for_status()
    return response.text
