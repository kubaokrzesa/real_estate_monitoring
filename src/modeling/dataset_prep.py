import sqlite3
import pandas as pd
from src.utils.get_config import config
from src.utils.get_config import feature_set_definitions
from sklearn.model_selection import train_test_split


def prepare_dataset(feature_set, surveys_to_use):
    num_cols_to_use = [f'n.{col}' for col in feature_set_definitions[feature_set].num_cols_to_use]
    geo_cols_to_use = [f'g.{col}' for col in feature_set_definitions[feature_set].geo_cols_to_use]
    cat_cols_to_use = [f'cf.{col}' for col in feature_set_definitions[feature_set].cat_cols_to_use]
    lab_cols_to_use = [f'l.{col}' for col in feature_set_definitions[feature_set].lab_cols_to_use]
    geo_dum_cols_to_use = [f'gd.{col}' for col in feature_set_definitions[feature_set].geo_dum_cols_to_use]

    base_cols = ['s.survey_id', 'n.link', 'n.sq_m_price']

    all_cols = base_cols + num_cols_to_use + geo_cols_to_use + cat_cols_to_use + lab_cols_to_use + geo_dum_cols_to_use
    all_cols_formatted = str(all_cols).replace('[', '').replace(']', '').replace("'", '')
    #
    query = f"""
    select {all_cols_formatted}
    from numeric_features n
    left join categorical_features cf on n.link = cf.link and n.survey_id = cf.survey_id
    left join label_features l on n.link = l.link and n.survey_id = l.survey_id
    left join geo_features g on n.link = g.link and n.survey_id = g.survey_id
    left join geo_dummy_features gd on n.link = gd.link and n.survey_id = gd.survey_id
    left join surveys s on n.survey_id = s.survey_id
    where s.survey_id in {str(surveys_to_use)} and
    s.survey_type in ('full', 'incremental') and
    n.sq_m_price not null and
    n.area > 10 and
    n.area < 1000 and
    n.sq_m_price > 100 and
    g.in_warsaw = True
    """

    with sqlite3.connect(config.sqlite_db) as conn:
        df = pd.read_sql_query(query, conn)
    df = df.set_index(['survey_id', 'link'])
    return df


def prepare_x_y(df):
    ## X y
    y = df['sq_m_price']
    X = df.drop(columns=['sq_m_price'])

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
    return X_train, X_test, y_train, y_test
