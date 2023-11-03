import requests
from scrapy import Selector
from bs4 import BeautifulSoup
import pandas as pd
from scraping import Scraper


scraper = Scraper(max_page_num=10)
scraper.collect_links_list()
scraper.save_link_list("initial_links.json")
scraper.extract_info_from_links()
scraper.save_results("offers.csv")





