import sqlite3
import pandas as pd

from src.utils.exceptions import SurveyNotFoundException


def check_if_survey_exists(survey_id, db):
    with sqlite3.connect(db) as conn:
        d = pd.read_sql_query(f"select count(*) survey from surveys where survey_id = '{survey_id}'", conn)
        if d['survey'].iloc[0] == 0:
            raise SurveyNotFoundException(f"Survey {survey_id} does not exist")


