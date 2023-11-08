import pandas as pd
import os

from src.pipeline.scraping import Scraper
from src.pipeline.data_cleaning import NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner
from src.pipeline.lat_lon_coding import LatLonCoder
from src.pipeline.geo_feature_extraction import GeoFeatureExtractor
from src.utils.get_config import config
from src.shp_file_downloader import get_shp_files
from src.pipeline.sqlite_creation import create_sqlite_db
from src.pipeline.survey_creation import SurveyCreator
import src.paths as paths


db = config.sqlite_db

os.makedirs(paths.data_directory, exist_ok=True)
create_sqlite_db(db)

create_survey = True

survey_type = 'test'
location = 'warsaw'

if create_survey:
    survey_creator = SurveyCreator(db, survey_type, location, max_page_num=config.max_page_num)
    survey_creator.create_survey_in_table()
    survey_creator.collect_links_list()
    survey_creator.clean_and_save_links_to_db()

    survey_id = survey_creator.survey_id
else:
    survey_id = config.survey_to_continue

if config.module_scraping:
    scraper = Scraper(max_page_num=config.max_page_num)
    scraper.execute_step()
    scraper.save_results(paths.scraping_output_path)

if config.module_data_cleaning:
    cleaners = [NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner]
    otp_paths = [paths.numeric_features_output_path, paths.categorical_features_output_path,
                 paths.label_features_output_path]
    df = pd.read_csv(paths.data_cleaning_input_path)

    for Cleaner, otp_path in zip(cleaners, otp_paths):
        cleaner = Cleaner()
        cleaner.load_previous_step_data(df)
        cleaner.execute_step()
        cleaner.save_results(otp_path)

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
