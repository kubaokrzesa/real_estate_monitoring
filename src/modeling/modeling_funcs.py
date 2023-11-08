import pandas as pd
import numpy as np

from sklearn.base import BaseEstimator, TransformerMixin

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