from src.modeling.dataset_prep import prepare_dataset, prepare_x_y
from src.modeling.modeling_funcs import DataPreprocessor, scoring_printout
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_absolute_percentage_error, mean_squared_error, \
    mean_absolute_error, median_absolute_error, max_error

from src.utils.get_config import modeling_config
import mlflow
import numpy as np
import os

from src.modeling.mlflow_utils import log_dataset_as_tags, log_regression_metrics

df = prepare_dataset(modeling_config.feature_set, modeling_config.surveys_to_use)
X_train, X_test, y_train, y_test = prepare_x_y(df)


# mlflow settings
mlflow.set_tracking_uri(modeling_config.mlflow_tracking_uri)
mlflow.set_experiment(modeling_config.mlflow_experiment_name)
with mlflow.start_run(run_name=modeling_config.mlflow_run_name):
    #

    # preprocessing
    dt_pr = DataPreprocessor()
    X_train = dt_pr.fit_transform(X_train)
    X_test = dt_pr.fit_transform(X_test)

    # modeling
    model = LinearRegression(**modeling_config.params)
    model.fit(X_train, y_train)
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)

    # logging
    mlflow.set_tag('description', modeling_config.description)
    log_regression_metrics(y_test, y_train, y_pred_test, y_pred_train)
    log_dataset_as_tags(modeling_config.feature_set, modeling_config.surveys_to_use,
                        y_train.name, X_train.columns)

    # score printouts
    scoring_printout(y_train, y_pred_train, y_test, y_pred_test)
    # Log parameters
    mlflow.log_params(modeling_config.params)

    # Log model
    mlflow.sklearn.log_model(model, modeling_config.output_model_name)

    # Log the training code
    if modeling_config.log_code_artifact:
        mlflow.log_artifact(os.path.abspath(__file__))
    #"src/modeling/model_trainings/linear_regression.py")

