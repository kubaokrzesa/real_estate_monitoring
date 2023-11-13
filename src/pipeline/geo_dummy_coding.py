import pandas as pd
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.pipeline_step_abc import PipelineStepABC
from src.utils.data_cleaning_funcs import normalize_categoricals
from typing import Tuple

logger = Logger(__name__).get_logger()


class GeoDummyCoder(PipelineStepABC):
    """
    Class for creating dummy variables for geographic features.

    This class is responsible for converting categorical geographic data into a one-hot encoded format,
    specifically for district names and metro proximity.

    Attributes:
        output_table (str): Name of the output table to store the processed data.
        query (str): SQL query to retrieve relevant geographic data.

    Methods:
        load_previous_step_data(): Loads data from the previous step and sets the DataFrame index.
        process(): Processes the geographic data to create dummy variables.
    """
    def __init__(self, db: str, survey_id: str):
        super().__init__(db, survey_id)
        self.output_table = 'geo_dummy_features'
        self.query = f"""select survey_id, link, warsaw_district, closest_metro 
        from geo_features where survey_id='{self.survey_id}'"""

    def load_previous_step_data(self):
        """
        Loads data from the previous step, sets the DataFrame index to 'survey_id' and 'link'.
        """
        super().load_previous_step_data()
        self.df = self.df.set_index(['survey_id', 'link'])

    def process(self):
        """
        Processes the geographic data to create one-hot encoded variables for districts and metro proximity.
        """
        districts = pd.get_dummies(self.df['warsaw_district'].str.lower().apply(normalize_categoricals), dtype=int)
        districts.columns = [f"di_{col}" for col in districts.columns]

        metro = pd.get_dummies(self.df['closest_metro'].str.lower().apply(normalize_categoricals), dtype=int)
        metro.columns = [f"mt_{col}" for col in metro.columns]
        ##

        self.df_out = pd.concat([districts, metro], axis=1)
        self.df_out = self.df_out.reset_index()
