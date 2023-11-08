from sklearn.model_selection import cross_val_score, KFold
import optuna
from optuna.visualization import plot_optimization_history
from optuna.visualization import plot_contour
from optuna.visualization import plot_param_importances
from optuna.visualization import plot_slice

from src.utils.setting_logger import Logger

from src.modeling.x_y_prep import X_train, X_test, y_train, y_test
from src.modeling.constants import (fe1, my_flat_fe1, suggested_base, base_cat_cols, fe1_xgb_best,
    object_cols, lat_lon_cols)

from src.modeling.modeling_funcs import scoring_printout_mod
import xgboost as xgb
import plotly.io as pio


logger = Logger(__name__).get_logger()

n_trials=250

fe_all = [col for col in list(X_train.columns) if col not in base_cat_cols + object_cols + lat_lon_cols]

feature_set = fe1


def objective(trial):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 400, 1200),
        'max_depth': trial.suggest_int('max_depth', 4, 18),
        'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3),
        'subsample': trial.suggest_float('subsample', 0.6, 0.85),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 0.9),
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 6),
        'reg_alpha': trial.suggest_float('reg_alpha', 0, 1),
        'reg_lambda': trial.suggest_float('reg_lambda', 1, 100)
    }

    model = xgb.XGBRegressor(**params)
    kf = KFold(n_splits=3)
    scores = cross_val_score(model, X_train[feature_set], y_train, cv=kf, scoring='neg_mean_squared_error', n_jobs=-1)
    rmse = (-scores.mean()) ** 0.5  # Negative MSE to positive RMSE
    return rmse

logger.info("launching optimisation")
study = optuna.create_study(direction='minimize')
study.optimize(objective, n_trials=n_trials)

best_params = study.best_params
best_score = study.best_value

logger.info(f"Best params: {best_params}")
logger.info(f"Best RMSE score: {best_score}")

best_model = xgb.XGBRegressor(**best_params)
best_model.fit(X_train[feature_set], y_train)

scoring_printout_mod(best_model, y_train, y_test, X_train[feature_set], X_test[feature_set])

hist = plot_optimization_history(study)
cont1 = plot_contour(study, params=['n_estimators', 'max_depth'])
cont2 = plot_contour(study, params=['reg_alpha', 'reg_lambda'])
imp = plot_param_importances(study)
sli = plot_slice(study)

pio.show(hist)
pio.show(cont1)
pio.show(cont2)
pio.show(imp)
pio.show(sli)