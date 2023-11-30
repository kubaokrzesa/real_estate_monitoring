import pytest
import numpy as np
import pandas as pd

from src.utils.data_cleaning_funcs import (normalize_categoricals, extract_floor,
    convert_col_to_num, separate_lables, extract_labels)


class TestNormalizeCategoricals:
    def test_null_input(self):
        assert normalize_categoricals(None) == 'MISSING'

    def test_empty_string(self):
        assert normalize_categoricals('') == 'MISSING'

    def test_polish_characters(self):
        assert normalize_categoricals('ąęćłńóśżź') == 'aeclnoszz'

    def test_non_alphanumeric(self):
        assert normalize_categoricals('hello!@#') == 'hello'

    def test_multiple_spaces(self):
        assert normalize_categoricals('hello  world') == 'hello_world'

    def test_mixed_conditions(self):
        assert normalize_categoricals('ąęć! łń óśżź  ') == 'aec_ln_oszz'

    def test_alphanumeric_no_change(self):
        assert normalize_categoricals('helloWorld') == 'helloWorld'

    def test_only_spaces(self):
        assert normalize_categoricals('     ') == 'MISSING'


class TestExtractFloor:
    def test_null_input(self):
        assert np.isnan(extract_floor(None))

    def test_no_floor_info(self):
        assert np.isnan(extract_floor("no floor info"))

    def test_single_floor(self):
        assert extract_floor("5") == 5
        assert extract_floor("parter") == 0

    def test_multiple_floor(self):
        assert extract_floor("3/5") == 3
        assert extract_floor("3/5", max_floor=True) == 5

    def test_non_standard_input(self):
        assert np.isnan(extract_floor("floor X"))
        assert extract_floor("parter/3", max_floor=True) == 3

    def test_return_type(self):
        assert isinstance(extract_floor("4"), int)
        assert np.isnan(extract_floor("not a floor"))


class TestConvertColToNum:
    def test_null_input(self):
        assert np.isnan(convert_col_to_num(None))

    def test_integer_input(self):
        assert convert_col_to_num(100) == pytest.approx(100)

    def test_float_input(self):
        assert convert_col_to_num(123.45) == pytest.approx(123.45)

    def test_string_with_integer(self):
        assert convert_col_to_num("123") == pytest.approx(123)

    def test_string_with_float(self):
        assert convert_col_to_num("123.45") == pytest.approx(123.45)

    def test_string_with_comma_decimal(self):
        assert convert_col_to_num("123,45") == pytest.approx(123.45)

    def test_non_numerical_string(self):
        assert np.isnan(convert_col_to_num("abc"))

    def test_string_with_mixed_characters(self):
        assert convert_col_to_num("123abc") == pytest.approx(123)

    def test_return_type(self):
        assert isinstance(convert_col_to_num("456"), float)
        assert np.isnan(convert_col_to_num("not a number"))

    def test_list(self):
        assert np.isnan(convert_col_to_num([]))


class TestSeparateLabels:
    def test_null_input(self):
        assert separate_lables(None) == ['NO']

    def test_empty_string(self):
        assert separate_lables('') == ['NO']

    def test_single_label(self):
        assert separate_lables('Label') == ['Label']

    def test_multiple_labels(self):
        assert separate_lables('Label1,Label2') == ['Label1', 'Label2']

    def test_labels_with_spaces(self):
        assert separate_lables(' Label1 , Label2 ') == ['Label1', 'Label2']

    def test_non_standard_characters(self):
        assert separate_lables('ąęćłńóśżź,Label') == ['aeclnoszz', 'Label']

    def test_special_characters(self):
        assert separate_lables('Label@#$,Another!Label') == ['Label', 'AnotherLabel']


class TestExtractLabels:
    def test_basic_functionality(self):
        df = pd.DataFrame({
            'link': ['link1', 'link2'],
            'labels': ['label1,label2', 'label3']
        })
        result = extract_labels(df, 'labels')
        expected_df = pd.DataFrame({
            'link': ['link1', 'link1', 'link2'],
            'labels_ls': ['label1', 'label2', 'label3']
        }).set_index('link')
        expected_df = pd.crosstab(expected_df.index, expected_df['labels_ls'])
        expected_df.index.name = 'link'
        expected_df.columns = [f"labels_{col}" for col in expected_df.columns]

        pd.testing.assert_frame_equal(result, expected_df)

    def test_empty_dataframe(self):
        df = pd.DataFrame({'link': [], 'labels': []})
        with pytest.raises(ValueError):
            extract_labels(df, 'labels')

    def test_no_matching_column(self):
        df = pd.DataFrame({'link': ['link1'], 'other': ['value']})
        with pytest.raises(ValueError):
            extract_labels(df, 'labels')

    def test_various_label_formats(self):
        df = pd.DataFrame({
            'link': ['link1', 'link2', 'link3'],
            'labels': ['label1, label2, label2', 'label3, label4', 'label1, label2, label3, label4']
        })
        result = extract_labels(df, 'labels')

        expected_df = pd.DataFrame({
            'link': ['link1', 'link2', 'link3'],
            'labels_label1': [1, 0, 1],
            'labels_label2': [2, 0, 1],
            'labels_label3': [0, 1, 1],
            'labels_label4': [0, 1, 1]
        }).set_index('link')
        pd.testing.assert_frame_equal(result, expected_df)

    def test_dataframe_with_missing_values(self):
        df = pd.DataFrame({
            'link': ['link1', 'link2', 'link3'],
            'labels': ['label1', None, '']
        })
        result = extract_labels(df, 'labels')
        expected_df = pd.DataFrame({
            'link': ['link1', 'link2', 'link3'],
            'labels_NO': [0, 1, 1],
            'labels_label1': [1, 0, 0]
        }).set_index('link')
        pd.testing.assert_frame_equal(result, expected_df)
