from src.modeling.dataset_prep import prepare_dataset, prepare_x_y
from src.modeling.modeling_funcs import DataPreprocessor
from sklearn.linear_model import LinearRegression

from src.utils.get_config import get_config_from_path
import os
from src.modeling.mlflow_utils import log_model_performance
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('TkAgg')

def plot_true_vs_pred(y_true, y_pred, plot_title, file_name):
    plt.figure(figsize=(8, 6))
    plt.scatter(y_true, y_pred, alpha=0.5)
    plt.xlabel('Actual Values')
    plt.ylabel('Predicted Values')
    plt.title(plot_title)
    plt.grid(True)
    plt.savefig(file_name)
    plt.show()
    plt.close()

training_config_path = 'configs/model_training_configs/modeling_config_lr.yaml'
modeling_config = get_config_from_path(training_config_path)
file_name = os.path.abspath(__file__)
plot_output_path = 'models/true_vs_pred_lr.png'

df = prepare_dataset(modeling_config.feature_set, modeling_config.surveys_to_use)
X_train, X_test, y_train, y_test = prepare_x_y(df)

# preprocessing
dt_pr = DataPreprocessor()
X_train = dt_pr.fit_transform(X_train)
X_test = dt_pr.fit_transform(X_test)

# modeling
model = LinearRegression(**modeling_config.params)
model.fit(X_train, y_train)
y_pred_train = model.predict(X_train)
y_pred_test = model.predict(X_test)

plot_true_vs_pred(y_test, y_pred_test, "true vs prdicted, test, lr", plot_output_path)

log_model_performance(model, X_train, y_train, X_test, y_test, modeling_config, file_name)

