import pandas as pd
import os

from src.utils.setting_logger import Logger
from src.pipeline.survey_creation import SurveyCreator
from src.pipeline.scraping import Scraper
from src.pipeline.data_cleaning import NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner
from src.pipeline.lat_lon_coding import LatLonCoder
from src.pipeline.geo_feature_extraction import GeoFeatureExtractor
from src.utils.get_config import config
from src.shp_file_downloader import get_shp_files
from src.pipeline.sqlite_creation import create_sqlite_db
from src.utils.db_utils import check_if_survey_exists
import src.paths as paths

logger = Logger(__name__).get_logger()

db = config.sqlite_db

os.makedirs(paths.data_directory, exist_ok=True)
create_sqlite_db(db)


if config.new_survey:
    survey_creator = SurveyCreator(db, config.survey_type, config.location, max_page_num=config.max_page_num)
    survey_creator.create_survey_in_table()
    survey_creator.collect_links_list()
    survey_creator.clean_and_save_links_to_db()

    survey_id = survey_creator.survey_id
else:
    survey_id = config.survey_to_continue
    check_if_survey_exists(survey_id, db)
    logger.info(f"Resuming survey: {survey_id}")

if config.module_scraping:
    scraper = Scraper(db=db, survey_id=survey_id)
    scraper.execute_step()

if config.module_data_cleaning:
    cleaners = [NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner]

    for Cleaner in cleaners:
        cleaner = Cleaner(db=db, survey_id=survey_id)
        cleaner.execute_step()

if config.module_lat_lon_coding:
    df_adr = pd.read_csv(paths.address_matching_input_path, usecols=['link', 'adress'])
    lat_lon_coder = LatLonCoder()
    lat_lon_coder.load_previous_step_data(df_adr)
    lat_lon_coder.execute_step()
    lat_lon_coder.save_results(paths.address_matching_output_path)

if config.module_get_shp_files:
    get_shp_files(config.shp_urls, config.shp_files_directory_name)

if config.module_extract_geo_features:
    gdf = pd.read_csv(paths.geo_features_input_path,
                      usecols=['link', 'address', 'latitude', 'longitude'])
    geo_feature_extractor = GeoFeatureExtractor()
    geo_feature_extractor.load_previous_step_data(gdf)
    geo_feature_extractor.execute_step()
    geo_feature_extractor.save_results(paths.geo_feature_output_path)
