import pandas as pd
import os

from src.pipeline.scraping import Scraper
from src.pipeline.lat_lon_coding import LatLonCoder
from src.pipeline.geo_feature_extraction import GeoFeatureExtractor
from src.utils.get_config import config
from src.shp_file_downloader import get_shp_files
from src.paths import (data_directory, scraping_output_path,
                       address_matching_output_path,
                       address_matching_input_path,
                       geo_features_input_path,
                       geo_feature_output_path)


os.makedirs(data_directory, exist_ok=True)

if config.module_scraping:
    scraper = Scraper(max_page_num=config.max_page_num)
    scraper.execute_step()
    scraper.save_results(scraping_output_path)

if config.module_lat_lon_coding:
    df_adr = pd.read_csv(address_matching_input_path, usecols=['link', 'adress'])
    lat_lon_coder = LatLonCoder()
    lat_lon_coder.load_previous_step_data(df_adr)
    lat_lon_coder.execute_step()
    lat_lon_coder.save_results(address_matching_output_path)

if config.module_get_shp_files:
    get_shp_files(config.shp_urls, config.shp_files_directory_name)

if config.module_extract_geo_features:
    gdf = pd.read_csv(geo_features_input_path,
                      usecols=['link', 'address', 'latitude', 'longitude'])
    geo_feature_extractor = GeoFeatureExtractor()
    geo_feature_extractor.load_previous_step_data(gdf)
    geo_feature_extractor.execute_step()
    geo_feature_extractor.save_results(geo_feature_output_path)
