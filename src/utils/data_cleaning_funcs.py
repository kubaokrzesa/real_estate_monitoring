import pandas as pd
import numpy as np
from datetime import datetime
from unidecode import unidecode
from typing import Optional
import re


def normalize_categoricals(text: Optional[str]) -> str:
    if text is None or pd.isna(text):
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


def extract_floor(val_str, max_floor=False):
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


def convert_col_to_num(val_str):
    if val_str is None or pd.isna(val_str):
        return np.nan
    elif type(val_str) in (int, float):
        return val_str
    else:
        numbers = re.findall('[\d,]+', val_str)
        numbers = ''.join(numbers)
        numbers = numbers.replace(',', '.')
        int_val = float(numbers)
        return int_val


def separate_lables(x):
    if x is None or pd.isna(x):
        otp = ['NO']
    else:
        otp = x.split(',')
        otp = [normalize_categoricals(text) for text in otp]
    return otp


def extract_labels(df, var):
    df_lab = df[['link', var]].set_index('link')

    df_lab[f"{var}_ls"] = df_lab[var].apply(separate_lables)
    df_lab = df_lab[f"{var}_ls"].explode().to_frame().reset_index()
    df_lab = pd.crosstab(df_lab['link'], df_lab[f'{var}_ls'])
    df_lab.columns.name = ''
    df_lab.columns = [f"{var}_{col}" for col in df_lab.columns]
    return df_lab
