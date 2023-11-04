import pandas as pd
import os
from pathlib import Path

from src.scraping import Scraper
from src.lat_lon_coding import LatLonCoder
from src.utils.get_config import config
from src.shp_file_downloader import get_shp_files


#
module_scraping = True
module_lat_lon_coding = True
module_get_shp_files = True

data_directory = Path('tests/data_test')
shp_files_directory_name = "tests/test_shp"

initial_links_path = data_directory / "initial_links.json"
scraping_output_path = data_directory / "offers.csv"
address_matching_output_path = data_directory / "geocoded_addresses.csv"

address_matching_input_path = data_directory / "offers.csv"
# "data/offers_full.csv"

max_page_num = 2 #243
#

os.makedirs(data_directory, exist_ok=True)

if module_scraping:
    scraper = Scraper(max_page_num=max_page_num)
    scraper.collect_links_list()
    scraper.save_link_list(initial_links_path)
    scraper.extract_info_from_links()
    scraper.save_results(scraping_output_path)

if module_lat_lon_coding:

    df_adr = pd.read_csv(address_matching_input_path, usecols=['link', 'adress'])

    lat_lon_coder = LatLonCoder(df=df_adr)
    lat_lon_coder.locate_addresses()
    lat_lon_coder.save_encoded_lat_lon(address_matching_output_path)

if module_get_shp_files:
    get_shp_files(config.shp_urls, shp_files_directory_name)
