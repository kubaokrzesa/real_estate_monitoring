

# Define the black-box function to optimize
def xgb_evaluate(max_depth, gamma, colsample_bytree, min_child_weight, subsample, n_estimators):
    params = {
        'eval_metric': 'rmse',
        'max_depth': int(max_depth),
        'min_child_weight':min_child_weight,
        'subsample': subsample,
        'eta': 0.1,
        'gamma': gamma,
        'colsample_bytree': colsample_bytree,
        'n_estimators': int(n_estimators)
    }
    # Cross validation with 5 folds
    cv_result = cross_val_score(xgb.XGBRegressor(**params), X_train, y_train, cv=5, scoring='r2', n_jobs=-1)
    # The Bayesian optimization library maximizes, so we need to negate the score here
    return np.mean(cv_result)

# Define the bounds of the hyperparameters to optimize
xgb_bo = BayesianOptimization(xgb_evaluate, {
    'max_depth': (3, 10),
    'min_child_weight': (1,10),
    'gamma': (0, 1),
    'colsample_bytree': (0.3, 0.9),
    'subsample': (0.5, 1),
    'n_estimators': (100, 1000)
})

# Run optimization
xgb_bo.maximize(init_points=5, n_iter=100)
xgb_bo.max['params']