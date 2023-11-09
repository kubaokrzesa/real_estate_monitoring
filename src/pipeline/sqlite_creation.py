import sqlite3
from datetime import datetime
import pandas as pd
from src.utils.setting_logger import Logger
from src.pipeline.sql_code import (survey_tab_drop, survey_tab_creation, survey_table_insert, survey_links_tab_creation,
                                   scraped_offers_tab_creation, numeric_feature_tab_creation,
                                   categorical_feature_tab_creation, label_feature_tab_creation)

logger = Logger(__name__).get_logger()


def create_sqlite_db(db):
    logger.info("Creating SQLite database if doesn't exist")

    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        # remove table with every run for testing purposes - REMOVE LATER
        # cur.execute(survey_tab_drop)
        # create table if exists
        cur.execute(survey_tab_creation)
        cur.execute(survey_links_tab_creation)
        cur.execute(scraped_offers_tab_creation)

        cur.execute(numeric_feature_tab_creation)
        cur.execute(categorical_feature_tab_creation)
        cur.execute(label_feature_tab_creation)


        conn.commit()
    logger.info("Database created successfully")
