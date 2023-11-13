import sqlite3
from src.utils.setting_logger import Logger
from src.db.sql_code import (survey_tab_creation, survey_links_tab_creation,
                             scraped_offers_tab_creation, numeric_feature_tab_creation,
                             categorical_feature_tab_creation, label_feature_tab_creation,
                             geocoded_adr_tab_creation, geo_feature_tab_creation)

logger = Logger(__name__).get_logger()


def create_sqlite_db(db: str) -> None:
    """
    Creates an SQLite database and initializes tables if they do not exist.

    This function connects to the specified SQLite database file and executes SQL statements
    to create various tables necessary for storing survey and real estate data.

    Args:
        db (str): The file path of the SQLite database.
    """
    logger.info("Creating SQLite database if doesn't exist")

    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        # create table if exists
        cur.execute(survey_tab_creation)
        cur.execute(survey_links_tab_creation)
        cur.execute(scraped_offers_tab_creation)

        cur.execute(numeric_feature_tab_creation)
        cur.execute(categorical_feature_tab_creation)
        cur.execute(label_feature_tab_creation)

        cur.execute(geocoded_adr_tab_creation)

        cur.execute(geo_feature_tab_creation)

        conn.commit()
    logger.info("Database created successfully")
