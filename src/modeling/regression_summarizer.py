"""
All regression diagnostics in one place!
"""
import pandas as pd
import shap
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.base import BaseEstimator
from src.modeling.modeling_funcs import scoring_printout
from sklearn.inspection import PartialDependenceDisplay
from typing import Optional, List, Union, Any, Callable
from sklearn import set_config
# here we call the new API set_config to tell sklearn we want to output a pandas DF
set_config(transform_output="pandas")


class RegressionSumarizer:
    def __init__(self, model: BaseEstimator, y_train: pd.Series, y_test: pd.Series,
                 X_train: pd.DataFrame, X_test: pd.DataFrame, transformer: Optional[Callable] = None,
                 target_transformer: Optional[Callable] = None,
                 inv_target_transformer: Optional[Callable] = None):
        """
        Initialize the RegressionSummarizer with a model and training/testing data.

        When working with multistep models like Pipeline or TransformedTargetRegressor:

        1. Give ONLY regressor model itself as a model argument, e.g. pipe.named_steps['model']
        2. Put all feature transformation steps as a single callable (NOT transformer object) as transformer
        3. If working with transformed target provide transformation function (callable) as target_transformer
        AND
        3.1 provide inverse transformation as inv_target_transformer

        Parameters:
        model (BaseEstimator): A fitted regression model.
        y_train (pd.Series): Training target values.
        y_test (pd.Series): Testing target values.
        X_train (pd.DataFrame): Training input features.
        X_test (pd.DataFrame): Testing input features.
        transformer (Callable, optional): function to preprocessing features (e.g. transformer.predict NOT transformer instance)
        target_transformer (Callable, optional): function to preprocessing features (e.g. transformer.predict NOT transformer instance)
        inv_target_transformer (Callable, optional): inverse target transform
        """
        self.model = model
        self.name = model.__class__.__name__.split('.')[-1]
        self.features = list(X_train.columns)
        self.transformer = transformer
        self.target_transformer = target_transformer
        self.inv_target_transformer = inv_target_transformer
        self.fe_imp_or_coef = self.get_feature_imp_or_coef(self.model, self.features)

        self.y_train = self.transform_y(y_train)
        self.y_test = self.transform_y(y_test)

        self.y_pred_train = model.predict(self.transform_X(X_train))
        self.y_pred_test = model.predict(self.transform_X(X_test))
        self.shap_explainer = self.get_shap_explainer(model, self.transform_X(X_test))


    def transform_X(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the transformation to the input features if a transformer is provided.

        Parameters:
        X (pd.DataFrame): The input features to transform.

        Returns:
        pd.DataFrame: Transformed features.
        """
        if self.transformer is None:
            return X
        else:
            idx = X.index
            cols = X.columns
            X = self.transformer(X)
            X = pd.DataFrame(X, columns=cols, index=idx)
            return X

    def transform_y(self, y: pd.Series) -> pd.Series:
        """
        Apply the transformation to the input features if a transformer is provided.

        Parameters:
        y (pd.Series): The target feature to transform.

        Returns:
        pd.Series: Transformed target.
        """
        if self.target_transformer is None:
            return y
        else:
            y = self.target_transformer(y)
            return y


    @staticmethod
    def get_shap_explainer(model: BaseEstimator, X: pd.DataFrame) -> shap.Explainer:
        """
        Get the appropriate SHAP explainer based on the model type.

        Parameters:
        model (BaseEstimator): A fitted regression model.
        X (pd.DataFrame): A sample of the input data.

        Returns:
        shap.Explainer: An appropriate SHAP explainer for the given model.
        """
        if hasattr(model, 'feature_importances_'):
            explainer = shap.TreeExplainer(model)
        elif hasattr(model, 'coef_'):
            explainer = shap.LinearExplainer(model, X)
        else:
            explainer = shap.Explainer(model.predict, X)
        return explainer

    @staticmethod
    def get_feature_imp_or_coef(model: BaseEstimator, features: List[str]) -> Optional[pd.Series]:
        """
        Get feature importances or coefficients from the model.

        Parameters:
        model (BaseEstimator): A fitted regression model.
        features (List[str]): List of feature names.

        Returns:
        Optional[pd.Series]: A pandas Series containing feature importances or coefficients.
        """
        if hasattr(model, 'feature_importances_'):
            imp = model.feature_importances_
            atr = 'feature_importances_'
        elif hasattr(model, 'coef_'):
            imp = model.coef_
            atr = 'coef_'
        else:
            imp = None
        if imp is not None:
            vals = pd.Series(imp, index=features, name=atr)
        else:
            vals = None
        return vals

        # metrics
    def print_scoring(self, inverse_y: bool = False):
        """
        Prints folowing metrics for testing and training set
        r2_score, mean_absolute_percentage_error, mean_squared_error, mean_absolute_error, median_absolute_error, max_error

        Parameters:
        inverse_y (bool): use inverse target transform (only works if provided)
        """
        if inverse_y:
            if self.inv_target_transformer is None:
                raise Exception("No inverse transformer provided")
            else:
                y_train = self.inv_target_transformer(self.y_train)
                y_pred_train = self.inv_target_transformer(self.y_pred_train)
                y_test = self.inv_target_transformer(self.y_test)
                y_pred_test = self.inv_target_transformer(self.y_pred_test)
                print("Scoring for original input space")
        else:
            y_train = self.y_train
            y_pred_train = self.y_pred_train
            y_test = self.y_test
            y_pred_test = self.y_pred_test
            if self.inv_target_transformer is not None:
                print("Scoring for transformed input space")

        scoring_printout(y_train, y_pred_train, y_test, y_pred_test)

    def get_correct_y(self, test: bool = True, inverse_y: bool = False):
        """
        Getting correct y vectors for plots. This function checks if y should be train or test and
        original space or transformed.

        Parameters:
        test (bool): if function should be executed for test data
        inverse_y (bool): use inverse target transform (only works if provided)

        Returns:
        pd.Series: A pandas Series with y true
        pd.Series: A pandas Series with y predicted
        str: train / test indicator
        Optional[str]: target space indicator
        """
        if test:
            y = self.y_test
            y_pred = self.y_pred_test
            dataset = 'test'
        else:
            y = self.y_train
            y_pred = self.y_pred_train
            dataset = 'train'

        if inverse_y:
            space = 'original space'
            if self.inv_target_transformer is None:
                raise Exception("No inverse transformer provided")
            else:
                y = self.inv_target_transformer(y)
                y_pred = self.inv_target_transformer(y_pred)
        else:
            if self.inv_target_transformer is None:
                space = None
            else:
                space = 'transformed space'
        return y, y_pred, dataset, space


    def plot_residual(self, test: bool = True, inverse_y: bool = False):
        """
        Plot the residuals for the test dataset.

        The residuals are calculated as the difference between the actual and predicted values.
        The plot is a histogram with a Kernel Density Estimate (KDE).

        Parameters:
        test (bool): if function should be executed for test data
        inverse_y (bool): use inverse target transform (only works if provided)
        """
        y, y_pred, dataset, space = self.get_correct_y(test, inverse_y)

        sns.histplot(y - y_pred, kde=True)
        if inverse_y:
            plt.title(f"actual - predicted, {dataset}")
        else:
            plt.title(f"actual - predicted, {dataset}, {space}")

        # true vs predicted
    def plot_true_vs_pred(self, test: bool = True, inverse_y: bool = False, figsize=(8, 6)):
        """
        Plot a scatter plot comparing actual and predicted values for the test set.

        The plot helps visualize the accuracy of the model on the test set.

        Parameters:
        test (bool): if function should be executed for test data
        inverse_y (bool): use inverse target transform (only works if provided)
        """
        y, y_pred, dataset, space = self.get_correct_y(test, inverse_y)

        plt.figure(figsize=figsize)
        plt.scatter(y, y_pred, alpha=0.5)
        plt.xlabel('Actual Values')
        plt.ylabel('Predicted Values')

        max_val = max(max(y), max(y_pred))
        plt.grid(True)
        plt.xlim(0, max_val)
        plt.ylim(0, max_val)
        plt.axline((0, 0), slope=1)
        if space is None:
            plot_title = f"true vs prdicted, {dataset}, {self.name}"
        else:
            plot_title = f"true vs prdicted, {dataset}, {self.name}, {space}"
        plt.title(plot_title)

        # importances
    def plot_fe_imp_or_coef(self, figsize=(6, 8)):
        """
        Plot feature importances (tree based models) or coefs (linear models)
        """
        if self.fe_imp_or_coef is None:
            raise Exception("Model does not have coef_ or feature_importance_")
        else:
            self.fe_imp_or_coef.sort_values().plot.barh(figsize=figsize)
            plt.title(f"{self.name}, {self.fe_imp_or_coef.name}")

    def plot_transformed_coef(self, figsize=(6, 8)):
        """
        Only meaningful for (regularized) linear regression with feature transformation
        """
        if self.fe_imp_or_coef is None:
            raise Exception("Model does not have coef_ or feature_importance_")
        else:
            coefs = self.fe_imp_or_coef.to_frame().T
            coefs_tr = self.transform_X(coefs)
            coefs_tr.T['coef_'].sort_values().plot.barh(figsize=figsize)
            plt.title(f"{self.name}, {self.fe_imp_or_coef.name}, Transformed coefs_")

        # shap
    def plot_shap_waterfall(self, case: pd.DataFrame, max_display: int = 14):
        """
        Plot a SHAP waterfall plot for a SINGLE observation.

        Parameters:
        case (pd.DataFrame): The input data with ONE observation for which SHAP values are to be calculated.
        max_display (int): The maximum number of features to display in the plot.
        """
        case = self.transform_X(case)
        explanation = self.shap_explainer(case)
        shap.waterfall_plot(explanation[0], max_display=max_display)

    def plot_shap_bee(self, X: pd.DataFrame, max_display: int = 14):
        """
        Plot a SHAP beeswarm plot for the input data.

        Parameters:
        X (pd.DataFrame): The input data for which SHAP values are to be calculated.
        max_display (int): The maximum number of features to display in the plot.
        """
        X = self.transform_X(X)
        explanation = self.shap_explainer(X)
        shap.plots.beeswarm(explanation, max_display=max_display)

        # works only with tree?
    def plot_shap_cluster(self, X: pd.DataFrame, y: pd.Series, clustering_cutoff: float = 0.5, max_display: int = 14):
        """
        Plot a SHAP bar plot with FEATURE clustering for the input data.

        Parameters:
        X (pd.DataFrame): The input data for which SHAP values are to be calculated.
        y (pd.Series): The target values.
        clustering_cutoff (float): The cutoff for clustering in the hierarchical clustering.
        max_display (int): The maximum number of features to display in the plot.
        """
        X = self.transform_X(X)
        y = self.transform_y(y)
        clustering = shap.utils.hclust(X, y)
        explanation = self.shap_explainer(X)
        shap.plots.bar(explanation, clustering=clustering, clustering_cutoff=clustering_cutoff, max_display=max_display)

    def plot_shap_cohort_bar(self, X: pd.DataFrame, gr_col: str, cohort_bar_cats: dict, max_display: int = 14):
        """
        Plot a SHAP bar plot for cohorts.

        Parameters:
        X (pd.DataFrame): The input data for which SHAP values are to be calculated.
        gr_col (str): Name of the feature used to define cohorts.
        cohort_bar_cats (dict): dictionary mapping values to cohort names e.g. {1:'center', 0:'not center'}
        max_display (int): The maximum number of features to display in the plot.
        """
        X = self.transform_X(X)
        explanation = self.shap_explainer(X)
        group = [cohort_bar_cats[1] if explanation[i,gr_col].data == 1 else cohort_bar_cats[0] for i in range(explanation.shape[0])]
        shap.plots.bar(explanation.cohorts(group).mean(0), max_display=max_display)

    def plot_shap_scatter(self, X: pd.DataFrame, col: Union[int, str]):
        """
        Plot a SHAP scatter plot for a specific feature.

        Parameters:
        X (pd.DataFrame): The input data for which SHAP values are to be calculated.
        col (Union[int, str]): The index or name of the feature for the scatter plot.
        """
        X = self.transform_X(X)
        explanation = self.shap_explainer(X)
        shap.plots.scatter(explanation[:, col], color=explanation)

    def plot_partial_dep(self, X: pd.DataFrame, var: Union[int, str], ice: bool = False, inverse_y: bool = False):
        """
        Plot a partial dependence plot for a specific feature.

        Parameters:
        X (pd.DataFrame): The input data.
        var (Union[int, str]): The index or name of the feature.
        ice (bool): Whether to use Individual Conditional Expectation (ICE) plots.
        inverse_y (bool): if y space should be transformed back to original (works for transformed target)
        """
        if inverse_y:
            if self.inv_target_transformer is None:
                raise Exception("No inverse transformer provided")
            else:
                prediction_func = lambda x: self.inv_target_transformer(self.model.predict(x))
        else:
            prediction_func = self.model.predict

        X = self.transform_X(X)
        shap.partial_dependence_plot(
        var,
        prediction_func,
        X,
        ice=ice,
        model_expected_value=True,
        feature_expected_value=True,
    )

    def plot_partial_dep2d(self, X: pd.DataFrame, var1: Union[int, str], var2: Union[int, str], width: int = 22, height: int = 6, inverse_y = False):
        """
        Plot a two-dimensional partial dependence plot for a pair of features.

        Parameters:
        X (pd.DataFrame): The input data.
        var1, var2 (Union[int, str]): The indices or names of the features.
        width (int): Width of the plot.
        height (int): Height of the plot.
        inverse_y (bool): NOT IMPLEMENTED YET
        """
        if inverse_y:
            raise NotImplementedError
        X = self.transform_X(X)
        pdd = PartialDependenceDisplay.from_estimator(self.model, X, [var1, var2, (var1, var2)])
        pdd.figure_.set_figwidth(width)
        pdd.figure_.set_figheight(height)
