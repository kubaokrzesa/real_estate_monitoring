import pytest
from pathlib import Path

from src.pipeline.scraping.scraping_funcs import extract_info_from_response

@pytest.fixture
def html_content():
    html_file_path = Path(__file__).parent / "example_htmls/example_html_1.txt"
    with open(html_file_path, 'r', encoding='utf-8') as file:
        return file.read()


@pytest.fixture
def invalid_html_content():
    html_file_path = Path(__file__).parent / "example_htmls/invalid_html_1.txt"
    with open(html_file_path, 'r', encoding='utf-8') as file:
        return file.read()


class TestExtractInfoFromResponse:
    def test_valid_html_content(self, html_content):
        url = "http://example.com"
        result = extract_info_from_response(html_content, url)

        assert type(result) == dict
        assert result['link'] == url
        assert result['title'] == "4pok., blisko centrum, zieleń, 3 tarasy, 2mp.+kom."
        assert result['price'] == "1 999 999 zł"
        assert result['adress'] == "al. Aleja Rzeczypospolitej, Błonia Wilanowskie, Wilanów, Warszawa, mazowieckie"
        assert result['sq_m_price'] == "20 528 zł/m²"
        assert result['area'] == "97,43 m²"
        assert result['ownership_type'] == "pełna własność"
        assert result['n_rooms'] == "4"
        assert result['state'] == "do zamieszkania"
        assert result['floor'] == "3/3"
        assert result['rent'] == "1 400 zł"
        assert result['remote'] == 'tak'
        assert result['balcony'] == "balkon, taras"
        assert result['heating'] == "miejskie"
        assert result['parking'] == "garaż/miejsce parkingowe"
        assert result['market'] == "wtórny"
        assert result['offerent_type'] == "prywatny"
        assert result['available_from'] == "brak informacji"
        assert result['year_built'] == "2018"
        assert result['building_type'] == "blok"
        assert result['windows'] == "drewniane"
        assert result['elevator'] == 'tak'
        assert result['media'] == 'telewizja kablowa, internet, telefon'
        assert result['safety'] == 'drzwi / okna antywłamaniowe, teren zamknięty, domofon / wideofon, monitoring / ochrona'
        assert result['equipment'] == 'zmywarka, lodówka, meble, piekarnik, kuchenka, telewizor, pralka'
        assert result['additional_info'] == 'piwnica, pom. użytkowe'
        assert result['building_material'] == 'beton'
        assert type(result['description']) == str and len(result['description']) > 0

    def test_invalid_html_content(self, invalid_html_content):
        url = "http://example.com"
        result = extract_info_from_response(invalid_html_content, url)

        assert type(result) == dict
        assert result['link'] == url
        assert result['title'] is None
        assert result['price'] is None
        assert result['adress'] is None
        assert result['sq_m_price'] is None
        assert result['area'] is None
        assert result['ownership_type'] is None
        assert result['n_rooms'] is None
        assert result['state'] is None
        assert result['floor'] is None
        assert result['rent'] is None
        assert result['remote'] is None
        assert result['balcony'] is None
        assert result['heating'] is None
        assert result['parking'] is None
        assert result['market'] is None
        assert result['offerent_type'] is None
        assert result['available_from'] is None
        assert result['year_built'] is None
        assert result['building_type'] is None
        assert result['windows'] is None
        assert result['elevator'] is None
        assert result['media'] is None
        assert result['safety'] is None
        assert result['equipment'] is None
        assert result['additional_info'] is None
        assert result['building_material'] is None
        assert result['description'] is None

