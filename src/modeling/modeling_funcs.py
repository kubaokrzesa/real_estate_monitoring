import pandas as pd
import numpy as np

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.impute import SimpleImputer

from sklearn.metrics import r2_score
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error, median_absolute_error, max_error


def scoring_printout(y_train, train_preds, y_test, test_preds):
    metrics = [r2_score, mean_absolute_percentage_error, mean_squared_error, mean_absolute_error, median_absolute_error, max_error]
    for metric in metrics:
        mt_val_train = metric(y_train, train_preds)
        mt_val_test = metric(y_test, test_preds)
        metric_name = metric.__name__
        if metric_name in [mean_squared_error.__name__]:
            mt_val_train = np.sqrt(mt_val_train)
            mt_val_test = np.sqrt(mt_val_test)
            metric_name = f"root_{metric_name}"

        print(f"Metric: {metric_name}:")
        print("train:")
        print(mt_val_train)
        print("test:")
        print(mt_val_test)
        print('')


def scoring_printout_mod(model, y_train, y_test, X_train, X_test):
    train_preds = model.predict(X_train)
    test_preds = model.predict(X_test)
    scoring_printout(y_train, train_preds, y_test, test_preds)


class CapTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, column, min_value=None, max_value=None):
        self.min_value = min_value
        self.max_value = max_value
        self.column = column

    def fit(self, X, y=None):
        # Compute the median for the values within the specified range and store it
        if self.min_value is not None and self.max_value is not None:
            mask = (X[self.column] >= self.min_value) & (X[self.column] <= self.max_value)
        elif self.max_value is not None:
            mask = X[self.column] <= self.max_value
        elif self.min_value is not None:
            mask = X[self.column] >= self.min_value
        else:
            mask = np.ones_like(X[self.column], dtype=bool)

        self.median_ = np.median(X[self.column][mask], axis=0)
        return self

    def transform(self, X):
        if self.min_value is not None:
            X[self.column] = np.where(X[self.column] > self.min_value, X[self.column], self.median_)
        if self.max_value is not None:
            X[self.column] = np.where(X[self.column] < self.max_value, X[self.column], self.median_)
        return X


class DataPreprocessor(BaseEstimator, TransformerMixin):
    def __init__(self, columns_to_impute_median=['floor', 'max_floor', 'age_num', 'n_rooms'],
                 zero_impute_columns=['rent', 'di_rembertow'],
                 age_num_bounds=(-5, 300)):
        self.columns_to_impute_median = columns_to_impute_median
        self.zero_impute_columns = zero_impute_columns
        self.age_num_bounds = age_num_bounds
        self.imputers_ = {}

    def fit(self, X, y=None):
        # Initialize the imputers
        for column in self.zero_impute_columns:
            self.imputers_[column] = SimpleImputer(strategy='constant', fill_value=0)
            self.imputers_[column].fit(X[[column]])

        for column in self.columns_to_impute_median:
            self.imputers_[column] = SimpleImputer(strategy='median')
            self.imputers_[column].fit(X[[column]])

        # Initialize and fit the cap transformer for 'age_num'
        self.age_cap_transformer_ = CapTransformer(column='age_num', min_value=self.age_num_bounds[0], max_value=self.age_num_bounds[1])
        self.age_cap_transformer_.fit(X)

        return self

    def transform(self, X):
        # Apply the imputers to transform the data
        for column, imputer in self.imputers_.items():
            X[column] = imputer.transform(X[[column]])

        # Apply the cap transformer
        X = self.age_cap_transformer_.transform(X)

        return X