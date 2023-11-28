import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sqlite3
from pandas.api.types import is_numeric_dtype, is_string_dtype

from src.db.sql_code import geo_dummy_tab_creation
from src.pipeline.geo_dummy_coding import GeoDummyCoder


db = Path(__file__).parent / 'geo_features_test.db'
survey_id = "test"


@pytest.fixture
def geo_dummy_coder():
    cleaner = GeoDummyCoder(db, survey_id)
    cleaner.load_previous_step_data()
    return cleaner


class TestCategoricalDataCleaner:

    def test_initialization(self, geo_dummy_coder):
        assert geo_dummy_coder.output_table == 'geo_dummy_features'

    def test_columns_present(self, geo_dummy_coder):
        geo_dummy_coder.process()
        # Assertions to verify the DataFrame has been processed as expected
        assert 'link' in geo_dummy_coder.df_out.columns
        assert 'di_srodmiescie' in geo_dummy_coder.df_out.columns

    def test_output_writing(self, geo_dummy_coder):
        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute(geo_dummy_tab_creation)
        geo_dummy_coder.execute_step()

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            res = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'geo_dummy_features'").fetchall()
            n_rows = cursor.execute(
                "SELECT count(*) FROM geo_dummy_features").fetchall()
        assert res[0][0] == 'geo_dummy_features'
        assert n_rows[0][0] > 0

        with sqlite3.connect(db) as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE geo_dummy_features")