import pandas as pd
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.pipeline_step_abc import PipelineStepABC
from src.utils.data_cleaning_funcs import normalize_categoricals

logger = Logger(__name__).get_logger()


class GeoDummyCoder(PipelineStepABC):
    def __init__(self, db, survey_id):
        super().__init__(db, survey_id)
        self.output_table = 'geo_dummy_features'
        self.query = f"""select survey_id, link, warsaw_district, closest_metro 
        from geo_features where survey_id='{self.survey_id}'"""

    def load_previous_step_data(self):
        super().load_previous_step_data()
        self.df = self.df.set_index(['survey_id', 'link'])

    def process(self):
        districts = pd.get_dummies(self.df['warsaw_district'].str.lower().apply(normalize_categoricals), dtype=int)
        districts.columns = [f"di_{col}" for col in districts.columns]

        metro = pd.get_dummies(self.df['closest_metro'].str.lower().apply(normalize_categoricals), dtype=int)
        metro.columns = [f"mt_{col}" for col in metro.columns]
        ##

        self.df_out = pd.concat([districts, metro], axis=1)
        self.df_out = self.df_out.reset_index()

