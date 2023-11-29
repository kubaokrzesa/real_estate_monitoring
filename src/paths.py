from pathlib import Path

data_directory = Path('data')
models_directory = Path('models')
assets_directory = Path('assets')
shape_files_directory = Path('shp_files')

metro_locations_path = assets_directory / 'metro_locations.csv'

powiaty_path = shape_files_directory / "powiaty"
gminy_path = shape_files_directory / "gminy"
jed_ewidencyjne_path = shape_files_directory / "jednostki_ewidencyjne"
