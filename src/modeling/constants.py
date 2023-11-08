import pandas as pd

suggested_base = {
    'ownership_type': 'pelna_wlasnosc',
    'state': 'do_zamieszkania',
    'remote': 'MISSING',
    'heating': 'miejskie',
    'parking': 'MISSING',
    'market': 'wtorny',
    'offerent_type': 'biuro_nieruchomosci',
    'building_type': 'blok',
    'windows': 'plastikowe',
    'elevator': 'nie',
    'building_material': 'brak_informacji'
}

base_cat_cols = []
for key, val in suggested_base.items():
    base_cat_cols.append(f"{key}_{val}")
base_cat_cols

my_flat_fe1 = pd.DataFrame({
    'area':65, 'n_rooms':3, 'rent':800, 'floor':2, 'max_floor':7, 'age_num':3,
       'center_dist':5.5, 'metro_dist':1, 'parking_garazmiejsce_parkingowe':1,
       'state_do_wykonczenia':0, 'market_wtorny':1, 'elevator_tak':1,
       'building_type_blok':0, 'building_type_kamienica':0, 'balcony_balkon':1,
       'media_internet':1, 'safety_monitoring_ochrona':1,
       'additional_info_piwnica':1, 'safety_teren_zamkniety':1, 'equipment_meble':1,
       'balcony_taras':0, 'balcony_ogrodek':0, 'di_bemowo':0, 'di_bialoleka':0,
       'di_bielany':0, 'di_ochota':0, 'di_praga_poludnie':0, 'di_pragapolnoc':0,
       'di_rembertow':0, 'di_srodmiescie':0, 'di_targowek':0, 'di_ursus':0,
       'di_ursynow':0, 'di_wawer':0, 'di_wesola':0, 'di_wilanow':0, 'di_wlochy':0,
       'di_wola':0, 'di_zoliborz':0

}, index=[0])

# fe1 definition
num_cols_to_use = ['area', 'n_rooms', 'rent', 'floor', 'max_floor', 'age_num']
# 'warsaw_district', closest_metro
geo_cols_to_use = ['center_dist', 'metro_dist']
cat_cols_to_use = ['parking_garazmiejsce_parkingowe', 'state_do_wykonczenia', 'market_wtorny', 'elevator_tak',
                   'building_type_blok', 'building_type_kamienica']
lab_cols_to_use = ['balcony_balkon', 'media_internet', 'safety_monitoring_ochrona', 'additional_info_piwnica',
                   'safety_teren_zamkniety', 'equipment_meble', 'balcony_taras', 'balcony_ogrodek']
districts_to_use = ['di_bemowo', 'di_bialoleka', 'di_bielany', 'di_ochota',
       'di_praga_poludnie', 'di_pragapolnoc', 'di_rembertow', 'di_srodmiescie',
       'di_targowek', 'di_ursus', 'di_ursynow', 'di_wawer', 'di_wesola',
       'di_wilanow', 'di_wlochy', 'di_wola', 'di_zoliborz']

fe1 = num_cols_to_use + geo_cols_to_use + cat_cols_to_use + lab_cols_to_use + districts_to_use
#

object_cols = ['address', 'warsaw_district', 'powiat', 'location', 'closest_metro']
lat_lon_cols = ['latitude', 'longitude'] # include???

fe1_xgb_best = {'n_estimators': 1160,
 'max_depth': 11,
 'learning_rate': 0.03923154850015585,
 'subsample': 0.8458070771157421,
 'colsample_bytree': 0.7733005900279369,
 'min_child_weight': 4,
 'reg_alpha': 0.7188450519517937,
 'reg_lambda': 15.49271495581602}