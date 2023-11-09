import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point

from src.utils.setting_logger import Logger
from src.utils.get_config import config
from src.pipeline.pipeline_step_abc import PipelineStepABC

logger = Logger(__name__).get_logger()


class GeoFeatureExtractor(PipelineStepABC):

    def __init__(self, db, survey_id):
        super().__init__(db=db, survey_id=survey_id)
        # TODO move to paths
        self.powiaty = gpd.read_file("shp_files/powiaty")[["JPT_NAZWA_", "geometry"]]
        self.gminy = gpd.read_file("shp_files/gminy")[["JPT_NAZWA_", "geometry"]]
        self.jednostki_ewidencyjne = gpd.read_file("shp_files/jednostki_ewidencyjne")[["JPT_NAZWA_", "geometry"]]
        self.warszawa_dzielnice = self.jednostki_ewidencyjne.sjoin(self.gminy[self.gminy.JPT_NAZWA_ == "Warszawa"],
                                                                   how="inner", predicate='within')
        self.warszawa_dzielnice = self.warszawa_dzielnice.drop(columns=['index_right', 'JPT_NAZWA__right'])
        self.warszawa_dzielnice = self.warszawa_dzielnice.rename(columns={'JPT_NAZWA__left': 'warsaw_district'})

        self.metro_stations = pd.read_csv("assets/metro_locations.csv", sep=';')
        self.metro_stations = convert_df_to_geo(self.metro_stations, self.warszawa_dzielnice.crs, base_crs='EPSG:4326')

        self.warsaw_center = Point(21.006209576726654, 52.230931183433256)
        self.warsaw_center = convert_point_to_crs(self.warsaw_center,
                                                  new_crs=self.warszawa_dzielnice.crs, base_crs='EPSG:4326')
        self.query = f"select * from geocoded_adr where survey_id = '{self.survey_id}'"
        self.output_table = 'geo_features'

    def load_previous_step_data(self):
        super().load_previous_step_data()
        self.df = convert_df_to_geo(self.df, self.warszawa_dzielnice.crs, base_crs='EPSG:4326')

    def process(self):
        gdf_augmented = self.df.sjoin(self.warszawa_dzielnice, how='left', predicate='within').drop(columns=['index_right'])
        gdf_augmented = gdf_augmented.sjoin(self.powiaty, how='left', predicate='within').drop(
            columns=['index_right']).rename(
            columns={"JPT_NAZWA_": 'powiat'})
        gdf_augmented['location'] = gdf_augmented.apply(
            lambda x: x['warsaw_district'] if x['warsaw_district'] is not np.nan else x['powiat'], axis=1)
        gdf_augmented['in_warsaw'] = gdf_augmented.apply(
            lambda x: True if x['warsaw_district'] is not np.nan else False, axis=1)
        gdf_augmented['center_dist'] = gdf_augmented['geometry'].distance(self.warsaw_center).div(1_000)
        metro_tab = find_closest_metro(gdf_augmented, self.metro_stations)
        gdf_augmented = gdf_augmented.merge(metro_tab, on='link', how='left')
        self.df_out = gdf_augmented.drop('geometry', axis=1)


def convert_df_to_geo(df, new_crs, base_crs='EPSG:4326'):
    df = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude))
    df = df.set_crs(base_crs, allow_override=True)
    df = df.to_crs(new_crs)
    return df


def convert_point_to_crs(point, new_crs, base_crs='EPSG:4326'):
    point_new_crs = gpd.GeoSeries([point], crs=base_crs).to_crs(new_crs).iloc[0]
    return point_new_crs


def find_closest_metro(gdf, metro_stations):
    offers = gdf[['link', 'geometry']]
    offers['key'] = 1

    metro_stations = metro_stations[['station_name', 'metro_line', 'geometry']]
    metro_stations['key'] = 1

    dist_tab = pd.merge(offers, metro_stations, on ='key', how='outer').drop(columns=['key'])
    dist_tab['distance'] = dist_tab['geometry_x'].distance(dist_tab['geometry_y']).div(1_000)

    dist_tab = dist_tab.pivot(index=['link'], columns=['station_name'], values='distance')

    closest_metro = dist_tab.apply(lambda x: x.idxmin(), axis=1)
    metro_dist = dist_tab.apply(lambda x: x.min(), axis=1)

    dist_tab['closest_metro'] = closest_metro
    dist_tab['metro_dist'] = metro_dist

    metro_tab = dist_tab.reset_index()[['link','closest_metro', 'metro_dist']]
    metro_tab.columns.name = ''
    return metro_tab
