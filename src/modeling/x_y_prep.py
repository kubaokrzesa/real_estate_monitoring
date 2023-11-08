import pandas as pd
import numpy as np
import os
from pathlib import Path
import re
from functools import reduce

from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from src.modeling.modeling_funcs import CapTransformer


data_directory = Path('data_full')
df_num = pd.read_csv(data_directory / "numeric_features_full.csv", sep=';').set_index('link')
df_cat = pd.read_csv(data_directory / "categorical_features_full.csv").set_index('link')
df_lab = pd.read_csv(data_directory / "label_features_full.csv").set_index('link')
df_geo = pd.read_csv(data_directory / "geo_features_full.csv").set_index('link')

# TODO move to preprocessing pipeline
from src.utils.data_cleaning_funcs import normalize_categoricals

districts = pd.get_dummies(df_geo['warsaw_district'].str.lower().apply(normalize_categoricals), dtype=int)
districts = districts.drop(columns=['MISSING', 'mokotow'])
districts.columns = [f"di_{col}" for col in districts.columns]

metro = pd.get_dummies(df_geo['closest_metro'].str.lower().apply(normalize_categoricals), dtype=int)
metro = metro.drop(columns=['MISSING', 'centrum'])
metro.columns = [f"mt_{col}" for col in metro.columns]
##

dfs = [df_num, df_geo, df_cat, df_lab, districts, metro]
dfs = [d.drop(columns=['Unnamed: 0']) if 'Unnamed: 0' in d.columns else d for d in dfs]
df = reduce(lambda a, b: a.merge(b, left_index=True, right_index=True, how='left'), dfs)

print(len(df))
exclusion_conds = df['sq_m_price'].isna() | (df['area'] < 10) | (df['area'] > 1_000) | (df['sq_m_price'] < 100)
df = df[~exclusion_conds]
print(len(df))
df = df[df['in_warsaw'] == True]
print(len(df))
df = df.drop(columns=['in_warsaw'])

## X y
y = df['sq_m_price']
X = df.drop(columns=['sq_m_price', 'price'])

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)

# Define which columns you want to impute with the median
columns_to_impute_median = ['floor', 'max_floor', 'age_num']

# Initialize a dictionary to store your imputers
imputers = {}

# Create and fit a zero imputer for 'rent'
zero_imputer = SimpleImputer(strategy='constant', fill_value=0)
zero_imputer.fit(X_train[['rent']])
imputers['rent'] = zero_imputer

# Create, fit and store median imputers for the other columns
for column in columns_to_impute_median:
    median_imputer = SimpleImputer(strategy='median')
    median_imputer.fit(X_train[[column]])
    imputers[column] = median_imputer

# Apply the imputers to transform the training data
X_train['rent'] = imputers['rent'].transform(X_train[['rent']])
for column in columns_to_impute_median:
    X_train[column] = imputers[column].transform(X_train[[column]])

# Apply the imputers to transform the test data
X_test['rent'] = imputers['rent'].transform(X_test[['rent']])
for column in columns_to_impute_median:
    X_test[column] = imputers[column].transform(X_test[[column]])


# fix age column
age_cap_transformer = CapTransformer(column = 'age_num',min_value=-5, max_value=300)
X_train = age_cap_transformer.fit_transform(X_train)

X_test = age_cap_transformer.transform(X_test)