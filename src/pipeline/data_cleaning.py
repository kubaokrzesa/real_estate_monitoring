import pandas as pd
import numpy as np
from datetime import datetime
from unidecode import unidecode
from typing import Optional
import re
import sqlite3

from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.pipeline_step_abc import PipelineStepABC
from src.utils.data_cleaning_funcs import (normalize_categoricals, extract_floor, convert_col_to_num,
                                           convert_col_to_num, separate_lables, extract_labels)

logger = Logger(__name__).get_logger()


class DataCleaner(PipelineStepABC):

    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.cat_vars = ['ownership_type', 'state', 'remote',
                         'heating', 'parking', 'market', 'offerent_type', 'building_type', 'windows',
                         'elevator', 'building_material']
        self.label_vars = ['balcony',
                           'media',
                           'safety',
                           'equipment',
                           'additional_info']
        self.num_cols = ['price', 'sq_m_price', 'area', 'n_rooms', 'rent']
        self.current_year = datetime.now().year

    def process(self):
        pass

    def execute_step(self):
        self.load_previous_step_data()
        self.process()
        self.upload_results_to_db()


class NumericDataCleaner(DataCleaner):
    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.output_table = 'numeric_features'

    def process(self):
        logger.info("Processing numerical columns")
        self.df_out = pd.DataFrame(index=self.df.index)
        self.df_out['link'] = self.df['link']

        for col in self.num_cols:
            self.df_out[col] = self.df[col].apply(convert_col_to_num)
        # process other cols that are numeric
        self.df_out['floor'] = self.df['floor'].apply(lambda x: extract_floor(x, max_floor=False))
        self.df_out['max_floor'] = self.df['floor'].apply(lambda x: extract_floor(x, max_floor=True))
        self.df_out['age_num'] = self.current_year - self.df['year_built'].replace('brak informacji', np.nan).astype(float)
        self.df_out['days_till_available'] = (
                    pd.to_datetime(self.df['available_from'].replace('brak informacji', np.nan)) - datetime.today()).dt.days


class CategoricalDataCleaner(DataCleaner):
    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.output_table = 'categorical_features'

    def process(self):
        logger.info("Processing categorical columns")
        self.df_out = pd.DataFrame(index=self.df.index)
        self.df_out['link'] = self.df['link']

        for categ in self.cat_vars:
            self.df_out[categ] = self.df[categ].apply(normalize_categoricals)

        self.df_out = pd.get_dummies(self.df_out.set_index('link'), dtype=int).reset_index()


class LabelDataCleaner(DataCleaner):
    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.output_table = 'label_features'

    def process(self):
        logger.info("Processing label columns")
        res_ls = []
        for var in self.label_vars:
            res_ls.append(extract_labels(self.df, var))
        self.df_out = pd.concat(res_ls, axis=1).reset_index()


