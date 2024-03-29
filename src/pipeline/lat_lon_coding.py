from typing import Any, Dict, List
import pandas as pd
from geopy.geocoders import Nominatim
from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.pipeline_step_abc import PipelineStepABC

logger = Logger(__name__).get_logger()


class LatLonCoder(PipelineStepABC):
    """
    A class for geocoding addresses into latitude and longitude coordinates.

    Attributes:
        db (str): Database address
        survey_id (str): survey to use
        output_table (str): Name of the table where output will be stored.
        query (str): SQL query to fetch relevant data.
        df_out (pandas.DataFrame): DataFrame to store output results.

    Methods:
        process(): Processes the addresses in the DataFrame and geocodes them.
    """
    def __init__(self, db: str, survey_id: str):
        super().__init__(db, survey_id)
        self.output_table = 'geocoded_adr'
        self.query = f"""select * from scraped_offers
         where survey_id='{self.survey_id}' and adress is not null"""

    @staticmethod
    def _locate_address(address: str) -> Dict[str, Any]:
        """
        Geocodes the given address into latitude and longitude.

        Args:
            address (str): The address to geocode.

        Returns:
            Dict[str, Any]: A dictionary with the address, latitude, and longitude.
        """
        logger.info(f"Finding latitude and longitude for: {address}")
        ### Ul. in address confuses geolocator
        address = address.replace("Ul. ", "").replace("ul. ", "").strip()
        logger.info(f"Cleaned address: {address}")
        geolocator = Nominatim(timeout=config.nominatim_timeout, user_agent=config.nominatim_user_agent)
        location = geolocator.geocode(address)
        if location is None:
            address = ", ".join(address.split(', ')[1:])
            logger.info(f"Making address wider: {address}")
            location = geolocator.geocode(address)

        if location:
            logger.info(f"latitude: {location.latitude}, longitude: {location.longitude}")
            res_dict = {'address': address, 'latitude': location.latitude, 'longitude': location.longitude}
            return res_dict
        else:
            logger.info(f"Location not found")
            res_dict = {'address': address, 'latitude': None, 'longitude': None}
            return res_dict

    def process(self) -> None:
        """
        Processes the addresses in the DataFrame and geocodes them.
        Updates the class's df_out attribute with the results.
        """
        total_cases = self.df.shape[0]
        cases_done = 0

        res_list = []
        try:
            for _, row in self.df.iterrows():
                try:
                    res = self._locate_address(row['adress'])
                    res['link'] = row['link']
                except Exception as e:
                    logger.info(f"Exception occured: {e}, Location not found")
                    res = {'address': row['adress'], 'latitude': None, 'longitude': None, 'link': row['link']}
                res_list.append(res)
                cases_done += 1
                logger.info(f"Processed {cases_done} / {total_cases} Cases")
        except KeyboardInterrupt:
            logger.info("User interrupted the process. Exiting...")
        finally:
            self.df_out = pd.DataFrame(res_list)

