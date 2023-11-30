import src.utils.get_config
from unittest.mock import patch
from box import Box
from src.special_steps.survey_creation import SurveyCreator

from src.db.sql_code import survey_links_tab_creation, survey_tab_creation

from pathlib import Path
import pandas as pd
import sqlite3
import os


db = Path(__file__).parent / 'scraping_test.db'


def test_survey_creation_async():
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        cur.execute(survey_tab_creation)
        cur.execute(survey_links_tab_creation)
        conn.commit()

    my_test_config = Box({'survey_type': 'test',
                          'location': 'warsaw',
                          'max_page_num': 50,
                          'use_async_links_scraping': True,
                          'base_link': "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/wiele-lokalizacji?locations=%5Bmazowieckie%2Cmazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%5D&viewType=listing&limit=72&page={}",
                          'headers': {
                              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
                          })
    with patch('src.special_steps.survey_creation.config', my_test_config):
        survey_creator = SurveyCreator(db, my_test_config.survey_type,
                                       my_test_config.location, max_page_num=my_test_config.max_page_num)
        survey_creator.create_survey_in_table()
        survey_creator.collect_links_list()
        survey_creator.clean_and_save_links_to_db()

    with sqlite3.connect(db) as conn:
        df_1 = pd.read_sql_query("select * from surveys", conn)
        df_2 = pd.read_sql_query("SELECT * FROM survey_links", conn)
    assert len(df_1) > 0
    assert len(df_2) > 0
    os.remove(db)


def test_survey_creation_non_async():
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()
        cur.execute(survey_tab_creation)
        cur.execute(survey_links_tab_creation)
        conn.commit()

    my_test_config = Box({'survey_type': 'test',
                          'location': 'warsaw',
                          'max_page_num': 5,
                          'use_async_links_scraping': False,
                          'base_link': "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/wiele-lokalizacji?locations=%5Bmazowieckie%2Cmazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%5D&viewType=listing&limit=72&page={}",
                          'headers': {
                              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
                          })

    with patch('src.special_steps.survey_creation.config', my_test_config):
        survey_creator = SurveyCreator(db, my_test_config.survey_type,
                                       my_test_config.location, max_page_num=my_test_config.max_page_num)
        survey_creator.create_survey_in_table()
        survey_creator.collect_links_list()
        survey_creator.clean_and_save_links_to_db()

    with sqlite3.connect(db) as conn:
        df_1 = pd.read_sql_query("select * from surveys", conn)
        df_2 = pd.read_sql_query("SELECT * FROM survey_links", conn)
    assert len(df_1) > 0
    assert len(df_2) > 0
    os.remove(db)
