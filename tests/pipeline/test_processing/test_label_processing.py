import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sqlite3
from pandas.api.types import is_numeric_dtype, is_string_dtype

from src.db.sql_code import label_feature_tab_creation
from src.pipeline.data_cleaning import LabelDataCleaner


db = Path(__file__).parent / 'scraped_offers_test.db'
survey_id = "test"


@pytest.fixture
def label_data_cleaner():
    cleaner = LabelDataCleaner(db, survey_id)
    cleaner.load_previous_step_data()
    return cleaner


class TestLabelDataCleaner:

    def test_initialization(self, label_data_cleaner):
        assert label_data_cleaner.output_table == 'label_features'

    def test_columns_present(self, label_data_cleaner):
        label_data_cleaner.process()
        # Assertions to verify the DataFrame has been processed as expected
        assert 'link' in label_data_cleaner.df_out.columns
        assert 'balcony_balkon' in label_data_cleaner.df_out.columns

    def test_output_writing(self, label_data_cleaner):
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(label_feature_tab_creation)
        label_data_cleaner.execute_step()

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            res = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'label_features'").fetchall()
            n_rows = cursor.execute(
                "SELECT count(*) FROM label_features").fetchall()
        assert res[0][0] == 'label_features'
        assert n_rows[0][0] > 0

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE label_features")