import os
from src.special_steps.survey_creation import SurveyCreator
from src.pipeline.scraping.scraping import Scraper
from src.pipeline.data_cleaning import NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner
from src.pipeline.lat_lon_coding import LatLonCoder
from src.pipeline.geo_feature_extraction import GeoFeatureExtractor
from src.pipeline.geo_dummy_coding import GeoDummyCoder
from src.special_steps.shp_file_downloader import get_shp_files
from src.db.sqlite_creation import create_sqlite_db
from src.utils.db_utils import check_if_survey_exists
from src.utils.funcs import check_if_shp_exists
import src.paths as paths
from pathlib import Path
from box import Box
from src.utils.setting_logger import Logger

from src.pipeline.scraping.async_scraping import AsyncScraper
import time
import pytest

logger = Logger(__name__).get_logger()

db = Path(__file__).parent / 'test.db'
shp_files_directory_name = Path(__file__).parent / "test_shp"

config = Box({'survey_type': 'test',
              'location': 'warsaw',
              'max_page_num': 2,
              'use_async_links_scraping': True,
              'base_link': "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/wiele-lokalizacji?locations=%5Bmazowieckie%2Cmazowieckie%2Fwarszawa%2Fwarszawa%2Fwarszawa%5D&viewType=listing&limit=72&page={}",
              'headers': {
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'},
                'nominatim_timeout': 10,
                'nominatim_user_agent': "kubaokrzesa",
                # shpefiles
                'shp_urls': ["https://www.gis-support.pl/downloads/2022/gminy.zip",
                  "https://www.gis-support.pl/downloads/2022/powiaty.zip",
                "https://www.gis-support.pl/downloads/2022/jednostki_ewidencyjne.zip"]
                      })


def test_e2e():
    logger.debug("Shapefiles")
    if not check_if_shp_exists(paths.shape_files_directory):
        get_shp_files(config.shp_urls, shp_files_directory_name)
    logger.debug("Shapefiles done")

    logger.debug("Creating db")
    create_sqlite_db(db)
    logger.debug("Db created")

    logger.debug("Creating survey")
    survey_creator = SurveyCreator(db, config.survey_type, config.location, max_page_num=config.max_page_num)
    survey_creator.create_survey_in_table()
    survey_creator.collect_links_list()
    survey_creator.clean_and_save_links_to_db()
    survey_id = survey_creator.survey_id
    logger.debug(f"Survey created: {survey_id}")

    logger.debug(f"Using regular scraper")
    scraper = Scraper(db=db, survey_id=survey_id)
    t1 = time.time()
    scraper.execute_step()
    t2 = time.time()
    scraping_time = t2-t1
    logger.debug(f"Scraping executed in {scraping_time}")

    logger.debug(f"Cleaning data")
    cleaners = [NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner]
    for Cleaner in cleaners:
        cleaner = Cleaner(db=db, survey_id=survey_id)
        cleaner.execute_step()
    logger.debug(f"Data cleaned")

    logger.debug(f"Lat lon coding")
    lat_lon_coder = LatLonCoder(db=db, survey_id=survey_id)
    lat_lon_coder.execute_step()
    logger.debug(f"Lat lon coding finished")

    logger.debug(f"Geo feature extraction")
    geo_feature_extractor = GeoFeatureExtractor(db=db, survey_id=survey_id)
    geo_feature_extractor.execute_step()
    logger.debug(f"Geo feature extraction finished")

    logger.debug(f"Geo feature dummy coding")
    geo_dummy_coder = GeoDummyCoder(db=db, survey_id=survey_id)
    geo_dummy_coder.execute_step()
    logger.debug(f"Geo feature dummy coding finished")
    os.remove(db)
    assert True
