import pandas as pd
from geopy.geocoders import Nominatim
from src.utils.setting_logger import Logger
import re
from src.utils.get_config import config

l = Logger(__name__)
logger = l.get_logger()


class LatLonCoder:

    def __init__(self, df):
        self.df = df
        self.df = self.df.dropna(subset=['adress'])
        self.encoded_addresses = None

    @staticmethod
    def _locate_address(address):
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

    def locate_addresses(self):
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
                    res = {'address': row['address'], 'latitude': None, 'longitude': None, 'link': row['link']}
                finally:
                    res_list.append(res)
                    cases_done += 1
                    logger.info(f"Processed {cases_done} / {total_cases} Cases")
        except KeyboardInterrupt:
            logger.info("User interrupted the process. Exiting...")
        finally:
            self.encoded_addresses = pd.DataFrame(res_list)

    def save_encoded_lat_lon(self, path):
        logger.info(f"Saving geocoding results to: {path}")
        self.encoded_addresses.to_csv(path)

