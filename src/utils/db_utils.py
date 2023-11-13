import sqlite3
import pandas as pd

from src.utils.exceptions import SurveyNotFoundException


def check_if_survey_exists(survey_id: str, db: str) -> None:
    """
    Checks if a survey with a given ID exists in the SQLite database.

    Connects to the database, queries for the survey, and raises an exception if the survey
    does not exist.

    Args:
        survey_id (str): The ID of the survey to check.
        db (str): The file path of the SQLite database.

    Raises:
        SurveyNotFoundException: If the survey with the given ID does not exist in the database.
    """
    with sqlite3.connect(db) as conn:
        d = pd.read_sql_query(f"select count(*) survey from surveys where survey_id = '{survey_id}'", conn)
        if d['survey'].iloc[0] == 0:
            raise SurveyNotFoundException(f"Survey {survey_id} does not exist")
