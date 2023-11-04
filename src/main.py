import pandas as pd
import os

from src.scraping import Scraper
from src.lat_lon_coding import LatLonCoder
from src.utils.get_config import config
from src.shp_file_downloader import get_shp_files
from src.paths import (data_directory, initial_links_path,
                       scraping_output_path,
                       address_matching_output_path,
                       address_matching_input_path)

os.makedirs(data_directory, exist_ok=True)

if config.module_scraping:
    scraper = Scraper(max_page_num=config.max_page_num)
    scraper.collect_links_list()
    scraper.save_link_list(initial_links_path)
    scraper.extract_info_from_links()
    scraper.save_results(scraping_output_path)

if config.module_lat_lon_coding:
    df_adr = pd.read_csv(address_matching_input_path, usecols=['link', 'adress'])

    lat_lon_coder = LatLonCoder(df=df_adr)
    lat_lon_coder.locate_addresses()
    lat_lon_coder.save_encoded_lat_lon(address_matching_output_path)

if config.module_get_shp_files:
    get_shp_files(config.shp_urls, config.shp_files_directory_name)
