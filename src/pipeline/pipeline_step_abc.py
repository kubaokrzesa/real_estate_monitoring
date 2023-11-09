from abc import ABC, abstractmethod
from src.utils.setting_logger import Logger
import pandas as pd
import sqlite3

logger = Logger(__name__).get_logger()


class PipelineStepABC(ABC):
    def __init__(self, db, survey_id):
        self.output_table = None
        self.df = None
        self.df_out = None
        self.db = db
        self.survey_id = survey_id
        self.query = f"select * from scraped_offers where survey_id='{self.survey_id}'"

    @abstractmethod
    def execute_step(self):
        """Perform main action of the transformation step"""
        pass

    def load_previous_step_data(self):
        with sqlite3.connect(self.db) as conn:
            self.df = pd.read_sql_query(self.query, conn)
        logger.info(f"Number of records loaded: {str(len(self.df))}")

    def upload_results_to_db(self):
        if 'survey_id' not in self.df_out.columns:
            self.df_out['survey_id'] = self.survey_id
            logger.info(self.survey_id)
        logger.info(f"Uploading results to database, table: {self.output_table}")
        logger.info(f"Number of records to upload: {str(len(self.df_out))}")
        with sqlite3.connect(self.db) as conn:
            self.df_out.to_sql(self.output_table, conn, if_exists='append', index=False)
        logger.info(f"Results uploaded successfully")

    def save_results(self, file_name):
        logger.info(f"Saving data table as {file_name}")
        self.df_out.to_csv(file_name)
