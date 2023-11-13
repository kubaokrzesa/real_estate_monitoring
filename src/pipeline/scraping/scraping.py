import requests
import pandas as pd
import time

from src.utils.exceptions import NoLinksException, UserInterruptException
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.scraping.scraping_funcs import extract_info_from_response

from src.pipeline.scraping.base_scraper import BaseScraper

logger = Logger(__name__).get_logger()


class Scraper(BaseScraper):

    def execute_step(self):
        self.load_previous_step_data()
        self.process()

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
                        finally:
                            continue
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


def fetch(link):
    logger.info(f"analyzing {link}")
    response = requests.get(link, headers=config.headers)
    return response.text
