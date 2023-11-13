import pandas as pd

import sqlite3
from src.utils.exceptions import AllLinksProcessedException, EmptySurveyException
from src.utils.setting_logger import Logger

from src.pipeline.pipeline_step_abc import PipelineStepABC

logger = Logger(__name__).get_logger()


class BaseScraper(PipelineStepABC):

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
        self.links = [f'https://www.otodom.pl/{link}' for link in self.links]

    def execute_step(self):
        pass

    def process(self):
        pass