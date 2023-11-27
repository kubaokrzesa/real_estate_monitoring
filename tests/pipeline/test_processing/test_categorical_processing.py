import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sqlite3
from pandas.api.types import is_numeric_dtype, is_string_dtype

from src.db.sql_code import categorical_feature_tab_creation
from src.pipeline.data_cleaning import CategoricalDataCleaner


db = Path(__file__).parent / 'scraped_offers_test.db'
survey_id = "test"


@pytest.fixture
def categorical_data_cleaner():
    cleaner = CategoricalDataCleaner(db, survey_id)
    cleaner.load_previous_step_data()
    return cleaner


class TestCategoricalDataCleaner:

    def test_initialization(self, categorical_data_cleaner):
        assert categorical_data_cleaner.output_table == 'categorical_features'

    def test_columns_present(self, categorical_data_cleaner):
        categorical_data_cleaner.process()
        # Assertions to verify the DataFrame has been processed as expected
        assert 'link' in categorical_data_cleaner.df_out.columns
        assert 'ownership_type_pelna_wlasnosc' in categorical_data_cleaner.df_out.columns

    def test_output_writing(self, categorical_data_cleaner):
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(categorical_feature_tab_creation)
        categorical_data_cleaner.execute_step()

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            res = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'categorical_features'").fetchall()
            n_rows = cursor.execute(
                "SELECT count(*) FROM categorical_features").fetchall()
        assert res[0][0] == 'categorical_features'
        assert n_rows[0][0] > 0

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE categorical_features")
