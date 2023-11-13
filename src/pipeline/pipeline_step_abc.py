from abc import ABC, abstractmethod
from src.utils.setting_logger import Logger
import pandas as pd
import sqlite3
from typing import Optional

logger = Logger(__name__).get_logger()


class PipelineStepABC(ABC):
    """
    Abstract base class for a pipeline step in a data processing pipeline.

    This class provides a template for a pipeline step that involves loading data,
    processing it, and then uploading the results to a database.

    Attributes:
        db (str): Database connection string.
        survey_id (str): ID of the survey being processed.
        output_table (Optional[str]): Name of the table where the output will be stored.
        df (Optional[pd.DataFrame]): DataFrame holding input data.
        df_out (Optional[pd.DataFrame]): DataFrame holding output data.
        query (str): SQL query for loading data.

    Methods:
        execute_step(): Executes the pipeline step.
        process(): Abstract method for processing data.
        load_previous_step_data(): Loads data from the previous step.
        validate_output_columns(): Validates and adjusts output DataFrame columns.
        upload_results_to_db(): Uploads the results to the database.
        save_results(file_name): Saves the DataFrame to a CSV file.
    """
    def __init__(self, db: str, survey_id: str):
        self.output_table: Optional[str] = None
        self.df: Optional[pd.DataFrame] = None
        self.df_out: Optional[pd.DataFrame] = None
        self.db: str = db
        self.survey_id: str = survey_id
        self.query = f"select * from scraped_offers where survey_id='{self.survey_id}'"

    def execute_step(self):
        """Executes the pipeline step by loading, processing, and uploading data."""
        self.load_previous_step_data()
        self.process()
        self.upload_results_to_db()

    @abstractmethod
    def process(self):
        """Perform main action of the transformation step"""
        pass

    def load_previous_step_data(self):
        """Loads data from a database based on the specified query into a DataFrame."""
        with sqlite3.connect(self.db) as conn:
            self.df = pd.read_sql_query(self.query, conn)
        logger.info(f"Number of records loaded: {str(len(self.df))}")

    def validate_output_columns(self):
        """Validates and adjusts the output DataFrame's columns to match the database schema."""
        if 'survey_id' not in self.df_out.columns:
            self.df_out['survey_id'] = self.survey_id
            logger.info(self.survey_id)

        with sqlite3.connect(self.db) as conn:
            col_names_db = pd.read_sql_query(f'PRAGMA table_info({self.output_table})', conn)['name']
        cols_not_in_db = list(set(self.df_out.columns).difference(set(col_names_db)))

        if cols_not_in_db:
            logger.info(f"Found columns not present in database table: {str(cols_not_in_db)}")
            self.df_out = self.df_out.drop(columns=cols_not_in_db)
        else:
            logger.info("All columns in the table present in database")

    def upload_results_to_db(self):
        """Validates output columns and uploads the results DataFrame to the specified database table."""
        self.validate_output_columns()
        logger.info(f"Uploading results to database, table: {self.output_table}")
        logger.info(f"Number of records to upload: {str(len(self.df_out))}")
        with sqlite3.connect(self.db) as conn:
            self.df_out.to_sql(self.output_table, conn, if_exists='append', index=False)
        logger.info(f"Results uploaded successfully")

    def save_results(self, file_name: str) -> None:
        """
        Saves the output DataFrame to a CSV file.

        Args:
            file_name (str): The name of the file to save the data to.
        """
        logger.info(f"Saving data table as {file_name}")
        self.df_out.to_csv(file_name)
