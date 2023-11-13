from sklearn.metrics import r2_score, mean_absolute_percentage_error, mean_squared_error, \
    mean_absolute_error, median_absolute_error, max_error
import mlflow
import numpy as np
from src.modeling.modeling_funcs import scoring_printout


def log_dataset_as_tags(feature_set, surveys_to_use, target_variable, x_variables):
    mlflow.set_tag("target_variable", target_variable)
    mlflow.set_tag("feature_set", feature_set)
    mlflow.set_tag("surveys_used", ', '.join(surveys_to_use) )
    mlflow.set_tag("feature_names", ', '.join(x_variables))


def log_regression_metrics(y_test, y_train, y_pred_test, y_pred_train):
    r2_test = r2_score(y_test, y_pred_test)
    r2_train = r2_score(y_train, y_pred_train)
    mape_test = mean_absolute_percentage_error(y_test, y_pred_test)
    mape_train = mean_absolute_percentage_error(y_train, y_pred_train)
    rmse_test = np.sqrt(mean_squared_error(y_test, y_pred_test))
    rmse_train = np.sqrt(mean_squared_error(y_train, y_pred_train))
    mae_test = mean_absolute_error(y_test, y_pred_test)
    mae_train = mean_absolute_error(y_train, y_pred_train)
    median_ae_test = median_absolute_error(y_test, y_pred_test)
    median_ae_train = median_absolute_error(y_train, y_pred_train)
    max_err_test = max_error(y_test, y_pred_test)
    max_err_train = max_error(y_train, y_pred_train)

    # Log the metrics
    mlflow.log_metrics({"r2_score_test": r2_test,
    "r2_score_train": r2_train,
    "mean_absolute_percentage_error_test": mape_test,
    "mean_absolute_percentage_error_train": mape_train,
    "root_mean_squared_error_test": rmse_test,
    "root_mean_squared_error_train": rmse_train,
    "mean_absolute_error_test": mae_test,
    "mean_absolute_error_train": mae_train,
    "median_absolute_error_test": median_ae_test,
    "median_absolute_error_train": median_ae_train,
    "max_error_test": max_err_test,
    "max_error_train": max_err_train})


def log_model_performance(model, X_train, y_train, X_test, y_test, modeling_config, file_name):
    # Making predictions
    y_pred_train = model.predict(X_train)
    y_pred_test = model.predict(X_test)
    # mlflow settings
    mlflow.set_tracking_uri(modeling_config.mlflow_tracking_uri)
    mlflow.set_experiment(modeling_config.mlflow_experiment_name)
    with mlflow.start_run(run_name=modeling_config.mlflow_run_name):
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
            mlflow.log_artifact(file_name)
