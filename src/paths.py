from pathlib import Path

data_directory = Path('data')

initial_links_path = data_directory / "initial_links.json"
scraping_output_path = data_directory / "offers.csv"

data_cleaning_input_path = scraping_output_path # "data_full/offers_full.csv"

numeric_features_output_path = data_directory / "numeric_features.csv"
categorical_features_output_path = data_directory / "categorical_features.csv"
label_features_output_path = data_directory / "label_features.csv"

address_matching_input_path = scraping_output_path # "data_full/offers_full.csv"
address_matching_output_path = data_directory / "geocoded_addresses.csv"


geo_features_input_path = address_matching_output_path #"data_full/geocoded_addresses_full.csv"
geo_feature_output_path = data_directory / "geo_features.csv"