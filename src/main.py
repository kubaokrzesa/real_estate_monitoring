import os

from src.utils.setting_logger import Logger
from src.special_steps.survey_creation import SurveyCreator
from src.pipeline.scraping import Scraper
from src.pipeline.data_cleaning import NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner
from src.pipeline.lat_lon_coding import LatLonCoder
from src.pipeline.geo_feature_extraction import GeoFeatureExtractor
from src.pipeline.geo_dummy_coding import GeoDummyCoder
from src.utils.get_config import config
from src.special_steps.shp_file_downloader import get_shp_files
from src.db.sqlite_creation import create_sqlite_db
from src.utils.db_utils import check_if_survey_exists
import src.paths as paths

from src.pipeline.experimental.async_scraping import AsyncScraper
import time

logger = Logger(__name__).get_logger()

db = config.sqlite_db

os.makedirs(paths.data_directory, exist_ok=True)
os.makedirs(paths.models_directory, exist_ok=True)
create_sqlite_db(db)

# TODO write automatic checker
if config.module_get_shp_files:
    get_shp_files(config.shp_urls, config.shp_files_directory_name)


if config.new_survey:
    survey_creator = SurveyCreator(db, config.survey_type, config.location, max_page_num=config.max_page_num)
    survey_creator.create_survey_in_table()
    survey_creator.collect_links_list()
    survey_creator.clean_and_save_links_to_db()

    survey_id = survey_creator.survey_id
else:
    survey_id = config.survey_to_continue
    check_if_survey_exists(survey_id, db)
    logger.info(f"Resuming survey: {survey_id}")

if config.module_scraping:
    if config.use_async_scraping:
        logger.info(f"Using async scraper")
        scraper = AsyncScraper(db=db, survey_id=survey_id)
    else:
        logger.info(f"Using regular scraper")
        scraper = Scraper(db=db, survey_id=survey_id)
    t1 = time.time()
    scraper.execute_step()
    t2 = time.time()
    scraping_time = t2-t1
    logger.info(f"Scraping executed in {scraping_time}")

if config.module_data_cleaning:
    cleaners = [NumericDataCleaner, CategoricalDataCleaner, LabelDataCleaner]

    for Cleaner in cleaners:
        if Cleaner.__name__ in config.excluded_cleaners:
            continue
        else:
            cleaner = Cleaner(db=db, survey_id=survey_id)
            cleaner.execute_step()

if config.module_lat_lon_coding:
    lat_lon_coder = LatLonCoder(db=db, survey_id=survey_id)
    lat_lon_coder.execute_step()

if config.module_extract_geo_features:
    geo_feature_extractor = GeoFeatureExtractor(db=db, survey_id=survey_id)
    geo_feature_extractor.execute_step()

if config.module_make_dummy_geo_features:
    geo_dummy_coder = GeoDummyCoder(db=db, survey_id=survey_id)
    geo_dummy_coder.execute_step()
