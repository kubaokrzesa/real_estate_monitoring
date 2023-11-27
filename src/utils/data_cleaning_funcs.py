import pandas as pd
import numpy as np
from datetime import datetime
from unidecode import unidecode
from typing import Optional, Union, List
import re


def normalize_categoricals(text: Optional[str]) -> str:
    """
    Normalizes categorical text data by removing Polish characters, non-alphanumeric characters,
    and spaces, then replacing spaces with underscores.

    Args:
        text (Optional[str]): The text to normalize.

    Returns:
        str: The normalized text.
    """
    if text is None or pd.isna(text) or text.isspace() or text == '':
        clean_text = 'MISSING'
    else:
        # remove Polish letters
        text_without_polish_chars = unidecode(text)
        # remove non alfanumeric and spaces
        clean_text = re.sub(r'[^a-zA-Z0-9 ]', '', text_without_polish_chars).strip()
        # remove multiple spaces
        clean_text = re.sub(r'\s+', ' ', clean_text)
        # replace spaces with _
        clean_text = re.sub(r' ', '_', clean_text)
    return clean_text


def extract_floor(val_str: Optional[str], max_floor: bool = False) -> Union[int, float]:
    """
    Extracts the floor number from a string, optionally extracting the maximum floor.

    Args:
        val_str (Optional[str]): The string containing the floor information.
        max_floor (bool): A flag indicating whether to extract the maximum floor.

    Returns:
        Union[int, float]: The extracted floor number, or NaN if not extractable.
    """
    if max_floor:
        idx = 1
    else:
        idx = 0
    if val_str is None or pd.isna(val_str):
        return np.nan
    elif '/' not in val_str:
        val_str = val_str.replace('parter', '0')
        val_str = re.findall('\d+', val_str)
        val_str = ''.join(val_str)
        if not val_str:
            return np.nan
        else:
            floor = int(val_str)
            return floor
    else:
        val_str = val_str.replace('parter', '0')
        floor = val_str.split('/')[idx]
        floor = re.findall('\d+', floor)
        if not floor:
            return np.nan
        floor = int(''.join(floor))
    return floor


def convert_col_to_num(val_str: Union[str, int, float]) -> Union[int, float]:
    """
    Converts a string containing a numerical value to a numerical data type.

    Args:
        val_str (Union[str, int, float]): The string to convert.

    Returns:
        Union[int, float]: The numerical value, or NaN if not convertible.
    """
    if val_str is None or pd.isna(val_str):
        return np.nan
    elif type(val_str) in (int, float):
        return val_str
    elif type(val_str) == str:
        val_str = val_str
    else:
        return np.nan

    numbers = re.findall(r'[\d,.]+', val_str)

    if len(numbers) == 0:
        return np.nan
    else:
        numbers = ''.join(numbers)
        numbers = numbers.replace(',', '.')
        int_val = float(numbers)
        return int_val


def separate_lables(x: Optional[str]) -> List[str]:
    """
    Separates and normalizes labels from a string.

    Args:
        x (Optional[str]): The string containing the labels.

    Returns:
        List[str]: A list of separated and normalized labels.
    """
    if x is None or pd.isna(x) or x == '' or x == 'MISSING':
        otp = ['NO']
    else:
        otp = x.split(',')
        otp = [normalize_categoricals(text) for text in otp]
    return otp


def extract_labels(df: pd.DataFrame, var: str) -> pd.DataFrame:
    """
    Extracts and transforms label data from a DataFrame into a format suitable for analysis.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        var (str): The name of the column containing label data.

    Returns:
        pd.DataFrame: A DataFrame with the transformed label data.
    """
    if len(df) == 0:
        raise ValueError("Input DataFrame is empty")
    if var not in df.columns:
        raise ValueError(f"{var} not present in columns")
    if 'link' not in df.columns:
        raise ValueError(f"df has not link column")

    df_lab = df[['link', var]].set_index('link')

    df_lab[f"{var}_ls"] = df_lab[var].apply(separate_lables)
    df_lab = df_lab[f"{var}_ls"].explode().to_frame().reset_index()
    df_lab = pd.crosstab(df_lab['link'], df_lab[f'{var}_ls'])
    df_lab.columns.name = ''
    df_lab.columns = [f"{var}_{col}" for col in df_lab.columns]
    return df_lab
