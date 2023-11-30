# real_estate_monitoring
Tool to scrape and analyse apartament offering prices in Warsaw.

Goal of the tool is to get all the real estate offers for Warsaw and build a model that predicts square meter price based on 
features like number of room, district, center proximity, availability of amenities etc. The model can help to estimate a fair price for your apartament,
find a best offer and understand the factors diving the price.

## The code has two main parts
* Data Scraping and processing pipeline
* Modeling pipeline

### Data Scraping and processing
* Run details cen be configured using config/config.yaml file
* to run pipeline use python src/main.py
* All results are stored in a sqlite database that is created on the first run.
* Every run creates new survey entry in db. All data has its survey_id as a key

#### Steps of pipeline
* module_get_shp_files - downloads shp files of Poland to be uses in further steps
* config.new_survey - creates survey in database and scrapes links to be processed
* module_scraping - scrapes links (possible to use async)
* module_data_cleaning - cleans data
* module_lat_lon_coding - encode adresses to latitude and longitude 
* module_extract_geo_features - extract information like districsts from lon, lat
* module_make_dummy_geo_features - encode districts and metro to dummy

### Modeling pipeline


# Access experiment tracking in MLflow with following command:
mlflow server --backend-store-uri=sqlite:///mlflow.db --default-artifact-root=file:mlruns --host 0.0.0.0 --port 500


# installing airflow
Make the script executable by running chmod +x install_airflow.sh.
Run the script with ./install_airflow.sh.

# change dag directory
nano ~/airflow/airflow.cfg