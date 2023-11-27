from scrapy import Selector
from bs4 import BeautifulSoup
from typing import Dict, Any


def extract_info_from_response(html: str, url: str) -> Dict[str, Any]:
    """
    Extracts information from the HTML response of a real estate listing.

    Parses the HTML content using both XPath and BeautifulSoup to extract various details
    of a real estate listing such as title, price, address, area, and other attributes.

    Args:
        html (str): The HTML content of the webpage.
        url (str): The URL of the webpage.

    Returns:
        Dict[str, Any]: A dictionary containing extracted information.

    Raises:
        ValueError: If the HTML content is not provided or is not a string.
    """
    if not html or not isinstance(html, str):
        raise ValueError(f"No HTML content to parse for link: {url}")
    selector = Selector(text=html)
    soup = BeautifulSoup(html, 'html.parser')
    result = {}
    try:
        result['link'] = url
        result['title'] = selector.xpath("//h1[@data-cy='adPageAdTitle']/text()").get()
        result['price'] = selector.xpath("//strong[@data-cy='adPageHeaderPrice']/text()").get()
        result['adress'] = selector.xpath("//a[@aria-label='Adres']/text()").get()
        result['sq_m_price'] = selector.xpath("//div[@aria-label='Cena za metr kwadratowy']/text()").get()

        result['area'] = selector.xpath('//div[@aria-label="Powierzchnia"]/div[2]/div[1]/text()').get()
        result['ownership_type'] = selector.xpath('//div[@aria-label="Forma własności"]/div[2]/div[1]/text()').get()
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
    except Exception as e:
        print(f"Exception occured: {e}")

    # extracting with beautiful soup
    # extracting n_room
    n_rooms_div = soup.find('div', attrs={"aria-label": "Liczba pokoi"})
    if n_rooms_div is None:
        result['n_rooms'] = None
    else:
        n_rooms_div = n_rooms_div.find('div', attrs={"class": 'css-1wi2w6s enb64yk5'})
        if n_rooms_div is not None:
            result['n_rooms'] = n_rooms_div.get_text(strip=True)
        else:
            result['n_rooms'] = None

    # extracting description
    description_div = soup.find('div', class_='css-1wekrze e1lbnp621')
    if description_div:
        result['description'] = description_div.get_text(strip=False)
    else:
        result['description'] = None

    return result
