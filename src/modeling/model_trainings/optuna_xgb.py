from src.modeling.dataset_prep import prepare_dataset, prepare_x_y
from src.modeling.modeling_funcs import DataPreprocessor
import xgboost as xgb
from pathlib import Path
from src.modeling.constants import my_flat_fe1
from src.utils.get_config import get_config_from_path
import os
import json
from src.modeling.mlflow_utils import log_model_performance, save_plot
from src.modeling.regression_summarizer import RegressionSumarizer
from src.modeling.optuna_opt import optimize_hyperparams_optuna
from joblib import dump
from optuna.visualization import plot_optimization_history
from optuna.visualization import plot_contour
from optuna.visualization import plot_param_importances
from optuna.visualization import plot_slice
import plotly.io as pio
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('TkAgg')


training_config_path = 'configs/model_training_configs/xgb_param_opt.yaml'
modeling_config = get_config_from_path(training_config_path)

output_dir_path = Path(modeling_config.output_dir)
file_name = os.path.abspath(__file__)
artifacts = []
artifacts.append(file_name)
artifacts.append(training_config_path)

# Dataset
df = prepare_dataset(modeling_config.feature_set, modeling_config.surveys_to_use)
X_train, X_test, y_train, y_test = prepare_x_y(df)

# preprocessing
dt_pr = DataPreprocessor()
X_train = dt_pr.fit_transform(X_train)
X_test = dt_pr.fit_transform(X_test)

# hyperparam opt
study = optimize_hyperparams_optuna(
    params_config=modeling_config.params_config,
    fixed_params=modeling_config.fixed_params,
    categorical_params=modeling_config.categorical_params,
    model_class=xgb.XGBRegressor,
    X=X_train,
    y=y_train,
    n_trials=modeling_config.n_trials,
    n_splits=modeling_config.n_splits,
    scoring=modeling_config.scoring,
    direction='maximize')

the_best_params = study.best_params

# TODO add optuna plots to artifacts and save
fig = plot_optimization_history(study)
fig.write_image(output_dir_path / "optimization_history.png")


with open(output_dir_path / f"{modeling_config.best_params_out_file}.json", "w") as outfile:
    json.dump(the_best_params, outfile)

# modeling
model = xgb.XGBRegressor(**the_best_params)
model.fit(X_train, y_train)
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

# Save the transformer and model
dump(dt_pr, output_dir_path / f'{modeling_config.transformer_model_name}.joblib')
dump(model, output_dir_path / f'{modeling_config.output_model_name}.joblib')

# summary
ms_xgb = RegressionSumarizer(model, y_train, y_test, X_train, X_test)

# plots
plots = [
    (ms_xgb.plot_fe_imp_or_coef, 'feature_importances', {}),
    (ms_xgb.plot_true_vs_pred, 'true_vs_pred', {'test': True}),
    (ms_xgb.plot_shap_waterfall, 'shap_my_flat', {'case': my_flat_fe1, 'max_display': 30}),
    (ms_xgb.plot_shap_bee, 'shap_bee', {'X': X_test, 'max_display': 30}),
    (ms_xgb.plot_shap_cluster, 'shap_cluster', {'X': X_test, 'y': y_test, 'max_display': 30}),
    (ms_xgb.plot_shap_scatter, 'shap_scatter', {'X': X_test, 'col': 'max_floor'}),
    (ms_xgb.plot_partial_dep, 'pdp_max_floor', {'X': X_test, 'var': 'max_floor', 'ice': False}),
    (ms_xgb.plot_partial_dep2d, 'pdp2_max_floor_rent', {'X': X_test, 'var1': 'max_floor', 'var2': 'rent'})
]

# Generate and save each plot
for plot_func, file_name, kwargs in plots:
    save_plot(plot_func, file_name, output_dir_path, **kwargs)

# logging
log_model_performance(model, X_train, y_train, X_test, y_test, modeling_config, artifacts)
