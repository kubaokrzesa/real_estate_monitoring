import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sqlite3
from pandas.api.types import is_numeric_dtype, is_string_dtype

from src.db.sql_code import numeric_feature_tab_creation
from src.pipeline.data_cleaning import NumericDataCleaner


db = Path(__file__).parent / 'scraped_offers_test.db'
survey_id = "test"


@pytest.fixture
def numeric_data_cleaner():
    cleaner = NumericDataCleaner(db, survey_id)
    cleaner.load_previous_step_data()
    return cleaner


class TestNumericDataCleaner:

    def test_initialization(self, numeric_data_cleaner):
        assert numeric_data_cleaner.output_table == 'numeric_features'

    def test_columns_present(self, numeric_data_cleaner):
        numeric_data_cleaner.process()
        assert 'link' in numeric_data_cleaner.df_out.columns
        assert 'sq_m_price' in numeric_data_cleaner.df_out.columns
        assert 'price' in numeric_data_cleaner.df_out.columns
        assert 'rent' in numeric_data_cleaner.df_out.columns
        assert 'floor' in numeric_data_cleaner.df_out.columns
        assert 'max_floor' in numeric_data_cleaner.df_out.columns
        assert 'age_num' in numeric_data_cleaner.df_out.columns
        assert 'days_till_available' in numeric_data_cleaner.df_out.columns

        assert is_string_dtype(numeric_data_cleaner.df_out['link'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['floor'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['max_floor'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['age_num'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['days_till_available'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['rent'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['price'])
        assert is_numeric_dtype(numeric_data_cleaner.df_out['sq_m_price'])

    def test_output_writing(self, numeric_data_cleaner):
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(numeric_feature_tab_creation)
        numeric_data_cleaner.execute_step()

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            res = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'numeric_features'").fetchall()
            n_rows = cursor.execute(
                "SELECT count(*) FROM numeric_features").fetchall()
        assert res[0][0] == 'numeric_features'
        assert n_rows[0][0] > 0

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE numeric_features")

