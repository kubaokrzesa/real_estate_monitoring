import pandas as pd

from scraping import Scraper
from lat_lon_coding import LatLonCoder

module_scraping = False
module_lat_lon_coding = True

initial_links_path = "initial_links.json"
scraping_output_path = "offers.csv"
address_matching_output_path = "geocoded_addresses.csv"

max_page_num = 2 #243


if module_scraping:
    scraper = Scraper(max_page_num=max_page_num)
    scraper.collect_links_list()
    scraper.save_link_list()
    scraper.extract_info_from_links(initial_links_path)
    scraper.save_results(scraping_output_path)

if module_lat_lon_coding:

    df_adr = pd.read_csv("data/offers_full.csv", usecols=['link', 'adress'])# , nrows=100

    lat_lon_coder = LatLonCoder(df=df_adr)
    lat_lon_coder.locate_addresses()
    lat_lon_coder.save_encoded_lat_lon(address_matching_output_path)










