from pathlib import Path

data_directory = Path('data')

initial_links_path = data_directory / "initial_links.json"
scraping_output_path = data_directory / "offers.csv"
address_matching_output_path = data_directory / "geocoded_addresses.csv"

address_matching_input_path = data_directory / "offers.csv"
# "data/offers_full.csv"
