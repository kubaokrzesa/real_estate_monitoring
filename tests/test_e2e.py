import pandas as pd
import os
from pathlib import Path

from src.pipeline.scraping import Scraper
from src.pipeline.lat_lon_coding import LatLonCoder
from src.pipeline.data_cleaning import NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner
from src.utils.get_config import config
from src.special_steps.shp_file_downloader import get_shp_files
from src.pipeline.geo_feature_extraction import GeoFeatureExtractor


#
module_scraping = True
module_data_cleaning = True
module_lat_lon_coding = True
module_get_shp_files = True
module_extract_geo_features = True

data_directory = Path('tests/data_test')
shp_files_directory_name = "tests/test_shp"

initial_links_path = data_directory / "initial_links.json"
scraping_output_path = data_directory / "offers.csv"

numeric_features_output_path = data_directory / "numeric_features.csv"
categorical_features_output_path = data_directory / "categorical_features.csv"
label_features_output_path = data_directory / "label_features.csv"

address_matching_output_path = data_directory / "geocoded_addresses.csv"

address_matching_input_path = data_directory / "offers.csv"

geo_features_input_path = data_directory / "geocoded_addresses.csv"
geo_feature_output_path = data_directory / "geo_features.csv"

max_page_num = 1 #243
#

os.makedirs(data_directory, exist_ok=True)

if module_scraping:
    scraper = Scraper(max_page_num=config.max_page_num)
    scraper.execute_step()
    scraper.save_results(scraping_output_path)

if module_data_cleaning:
    data_cleaning_input_path = scraping_output_path
    cleaners = [NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner]
    otp_paths = [numeric_features_output_path, categorical_features_output_path,
                 label_features_output_path]
    df = pd.read_csv(data_cleaning_input_path)

    for Cleaner, otp_path in zip(cleaners, otp_paths):
        cleaner = Cleaner()
        cleaner.load_previous_step_data(df)
        cleaner.execute_step()
        cleaner.save_results(otp_path)

if module_lat_lon_coding:
    df_adr = pd.read_csv(address_matching_input_path, usecols=['link', 'adress'])
    lat_lon_coder = LatLonCoder()
    lat_lon_coder.load_previous_step_data(df_adr)
    lat_lon_coder.execute_step()
    lat_lon_coder.save_results(address_matching_output_path)

if module_get_shp_files:
    get_shp_files(config.shp_urls, shp_files_directory_name)

if module_extract_geo_features:
    gdf = pd.read_csv(geo_features_input_path,
                      usecols=['link', 'address', 'latitude', 'longitude'])
    geo_feature_extractor = GeoFeatureExtractor()
    geo_feature_extractor.load_previous_step_data(gdf)
    geo_feature_extractor.execute_step()
    geo_feature_extractor.save_results(geo_feature_output_path)