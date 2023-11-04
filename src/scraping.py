import requests
from scrapy import Selector
from bs4 import BeautifulSoup
import pandas as pd
import time
import json

from src.utils.exceptions import NoLinksException
from src.utils.setting_logger import Logger
from src.utils.get_config import config

l = Logger(__name__)
logger = l.get_logger()


class Scraper:

    def __init__(self, max_page_num=10):
        self.df = None
        self.links = []
        # TODO: automatic max page num finder
        self.max_page_num = max_page_num

    def collect_links_list(self):
        logger.info(f"Collecting links to real estate offers")
        for page_num in range(self.max_page_num):
            # TODO: move to config
            base_link = f"https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/wiele-lokalizacji?locations=%5Bmazowieckie%2Cmazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%5D&viewType=listing&limit=72&page={str(page_num)}"
            url = base_link
            logger.info(f"Visiting page with offers number {str(page_num)}, url: {url}")

            response = requests.get(url, headers=config.headers)
            selector = Selector(response)

            listings = selector.xpath("//a[@data-cy='listing-item-link']/@href")

            links_list = [listing.get() for listing in listings]
            self.links.extend(links_list)
        logger.info(f"Total number of links collected: {str(len(self.links))}")

    def save_link_list(self, file_name):
        logger.info(f"Saving links list as {file_name}")
        with open(file_name, 'w') as file:
            json.dump(self.links, file)

    def load_link_list(self, file_name):
        logger.info(f"Loading links list from {file_name}")
        with open(file_name, 'r') as file:
            self.links = json.load(file)

    def extract_info_from_links(self):
        logger.info(f"Iterating links to extract information")
        if not self.links:
            raise NoLinksException("The list of links is empty, fill it before iterating")

        res_ls = []
        try:
            while self.links:
                link = self.links.pop()
                logger.info(f"Analysing link: {link}")
                try:
                    extracted_info = _extract_info_from_link(link)
                    res_ls.append(extracted_info)
                except Exception as e:
                    logger.info(f"Exception occurred: {str(e)}, retrying")
                    for i in range(1, 11):
                        logger.info(f"Pausing for {str(i)} seconds")
                        time.sleep(i)
                        try:
                            extracted_info = _extract_info_from_link(link)
                            res_ls.append(extracted_info)
                            break
                        finally:
                            pass
                    if i == 10:
                        logger.info(f"Maximum retries reached, breaking")
                        self.save_link_list("remaining_links.json")
                        break
        except KeyboardInterrupt:
            logger.info("User interrupted the process. Exiting...")
            self.save_link_list("remaining_links.json")
        finally:
            self.df = pd.DataFrame(res_ls)

    def save_results(self, file_name):
        logger.info(f"Saving data table as {file_name}")
        self.df.to_csv(file_name)


def _extract_info_from_link(link):
    link_full = 'https://www.otodom.pl/' + link

    logger.info(f"analyzing {link_full}")

    response = requests.get(link_full, headers=config.headers)
    selector = Selector(response)

    result = {}

    result['link'] = link_full
    result['title'] = selector.xpath("//h1[@data-cy='adPageAdTitle']/text()").get()
    result['price'] = selector.xpath("//strong[@data-cy='adPageHeaderPrice']/text()").get()
    result['adress'] = selector.xpath("//a[@aria-label='Adres']/text()").get()
    result['sq_m_price'] = selector.xpath("//div[@aria-label='Cena za metr kwadratowy']/text()").get()

    result['area'] = selector.xpath('//div[@aria-label="Powierzchnia"]/div[2]/div[1]/text()').get()
    result['ownership_type'] = selector.xpath('//div[@aria-label="Forma własności"]/div[2]/div[1]/text()').get()
    result['n_rooms'] = selector.xpath('//div[@aria-label="Liczba pokoi"]/div[2]/div[1]/text()').get()
    result['state'] = selector.xpath('//div[@aria-label="Stan wykończenia"]/div[2]/div[1]/text()').get()
    result['floor'] = selector.xpath('//div[@aria-label="Piętro"]/div[2]/div[1]/text()').get()
    result['rent'] = selector.xpath('//div[@aria-label="Czynsz"]/div[2]/div[1]/text()').get()
    result['remote'] = selector.xpath('//div[@aria-label="Obsługa zdalna"]/div[2]/div[1]/text()').get()
    result['balcony'] = selector.xpath('//div[@aria-label="Balkon / ogród / taras"]/div[2]/div[1]/text()').get()
    result['heating'] = selector.xpath('//div[@aria-label="Ogrzewanie"]/div[2]/div[1]/text()').get()
    result['parking'] = selector.xpath('//div[@aria-label="Miejsce parkingowe"]/div[2]/div[1]/text()').get()

    result['market'] = selector.xpath('//div[@aria-label="Rynek"]/div[2]/div[1]/text()').get()
    result['offerent_type'] = selector.xpath('//div[@aria-label="Typ ogłoszeniodawcy"]/div[2]/div[1]/text()').get()
    result['available_from'] = selector.xpath('//div[@aria-label="Dostępne od"]/div[2]/div[1]/text()').get()
    result['year_built'] = selector.xpath('//div[@aria-label="Rok budowy"]/div[2]/div[1]/text()').get()
    result['building_type'] = selector.xpath('//div[@aria-label="Rodzaj zabudowy"]/div[2]/div[1]/text()').get()
    result['windows'] = selector.xpath('//div[@aria-label="Okna"]/div[2]/div[1]/text()').get()
    result['elevator'] = selector.xpath('//div[@aria-label="Winda"]/div[2]/div[1]/text()').get()
    result['media'] = selector.xpath('//div[@aria-label="Media"]/div[2]/div[1]/text()').get()
    result['safety'] = selector.xpath('//div[@aria-label="Zabezpieczenia"]/div[2]/div[1]/text()').get()
    result['equipment'] = selector.xpath('//div[@aria-label="Wyposażenie"]/div[2]/div[1]/text()').get()
    result['additional_info'] = selector.xpath(
        '//div[@aria-label="Informacje dodatkowe"]/div[2]/div[1]/text()').get()
    result['building_material'] = selector.xpath('//div[@aria-label="Materiał budynku"]/div[2]/div[1]/text()').get()

    # extracting description
    soup = BeautifulSoup(response.content, 'html.parser')
    description_div = soup.find('div', class_='css-1wekrze e1lbnp621')
    if description_div:
        result['description'] = description_div.get_text(strip=False)
    else:
        result['description'] = None

    return result
