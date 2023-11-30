from src.pipeline.scraping.scraping import Scraper
from src.pipeline.scraping.async_scraping import AsyncScraper
from src.db.sql_code import survey_links_tab_creation, scraped_offers_tab_creation

from pathlib import Path
import pandas as pd
import sqlite3
import os


base_links_path = Path(__file__).parent / 'base_links' / 'test_survey_links.csv'
db = Path(__file__).parent / 'scraping_test.db'
survey_id = "test"

survey_links = pd.read_csv(base_links_path)


def test_regular_scraper():
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        # create table if exists
        cur.execute(survey_links_tab_creation)
        cur.execute(scraped_offers_tab_creation)
        survey_links.to_sql('survey_links', conn, if_exists='append', index=False)
        conn.commit()
        df = pd.read_sql_query("select * from scraped_offers", conn)
    assert len(df) == 0

    scraper = Scraper(db=db, survey_id=survey_id)
    scraper.execute_step()

    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query("select * from scraped_offers", conn)
    assert len(df) > 0
    os.remove(db)


def test_async_scraper():
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        # create table if exists
        cur.execute(survey_links_tab_creation)
        cur.execute(scraped_offers_tab_creation)
        survey_links.to_sql('survey_links', conn, if_exists='append', index=False)
        conn.commit()
        df = pd.read_sql_query("select * from scraped_offers", conn)
    assert len(df) == 0

    scraper = AsyncScraper(db=db, survey_id=survey_id)
    scraper.execute_step()

    with sqlite3.connect(db) as conn:
        df = pd.read_sql_query("select * from scraped_offers", conn)
    assert len(df) > 0
    os.remove(db)
