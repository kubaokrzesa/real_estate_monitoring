"""
Funcion to optimize hyperparameters using Optuna
"""
from optuna.visualization import plot_optimization_history
from optuna.visualization import plot_contour
from optuna.visualization import plot_param_importances
from optuna.visualization import plot_slice
import plotly.io as pio

import optuna
from sklearn.model_selection import cross_val_score, KFold
from typing import Dict, Any, List, Tuple, Type
from sklearn.base import BaseEstimator
import pandas as pd
import numpy as np


def optimize_hyperparams_optuna(
    params_config: Dict[str, Dict[str, Any]],
    fixed_params: Dict[str, Any],
    categorical_params: List[Tuple[str, List[Any]]],
    model_class: Type[BaseEstimator],
    X: pd.DataFrame,
    y: np.ndarray,
    n_trials: int = 100,
    n_splits: int = 3,
    scoring: str = 'neg_mean_squared_error',
    direction: str = 'maximize'
) -> optuna.study.Study:
    """
    Optimize hyperparameters for a given machine learning model using Optuna.

    This function creates an Optuna study to find the best hyperparameters for a model.
    It supports optimizing continuous (float, int) and categorical parameters.
    The optimization process uses K-Fold cross-validation to evaluate the performance of the model.

    Parameters:
    - params_config (Dict[str, Dict[str, Any]]): Configuration for hyperparameters to optimize.
      Each key is a parameter name, and the value is a dictionary with keys 'type' and 'range'.
    - fixed_params (Dict[str, Any]): Fixed parameters that are not optimized.
    - categorical_params (List[Tuple[str, List[Any]]]): List of categorical parameters and their possible values.
    - model_class (Type[BaseEstimator]): The class of the model to be optimized.
    - X (pd.DataFrame): Feature dataset.
    - y (np.ndarray): Target variable.
    - n_trials (int, optional): The number of trials for optimization. Default is 100.
    - n_splits (int, optional): The number of splits for K-Fold cross-validation. Default is 3.
    - scoring (str, optional): Scoring metric for cross-validation. Default is 'neg_mean_squared_error'.
    - direction (str, optional): The direction of optimization ('minimize' or 'maximize'). Default is 'maximize'.

    Returns:
    optuna.study.Study: The Optuna study object, which contains the results of the optimization.

    Example Usage:
    >>> study = optimize_hyperparams_optuna(params_config, fixed_params, categorical_params, model_class, X, y)
    >>> best_params = study.best_params
    >>> print(f"Best parameters: {best_params}")
    """
    def get_objective(params_config, fixed_params, categorical_params, model_class, X, y, n_splits, scoring):
        def objective(trial):
            params = {}
            for param, config in params_config.items():
                if config['type'] == 'float':
                    params[param] = trial.suggest_float(param, *config['range'])
                elif config['type'] == 'int':
                    params[param] = trial.suggest_int(param, *config['range'])
            params.update(fixed_params)

            if categorical_params:
                for categorical_param in categorical_params:
                    params[categorical_param[0]] = trial.suggest_categorical(categorical_param[0], categorical_param[1])

            model = model_class(**params)
            kf = KFold(n_splits=n_splits)
            scores = cross_val_score(model, X, y, cv=kf, scoring=scoring, n_jobs=-1)
            metric = scores.mean()
            return metric
        return objective

    study = optuna.create_study(direction=direction)
    objective_ = get_objective(params_config, fixed_params, categorical_params, model_class, X, y, n_splits, scoring)
    study.optimize(objective_, n_trials=n_trials)

    return study
