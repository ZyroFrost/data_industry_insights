import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.svm import SVR
from sklearn.metrics import mean_absolute_error, mean_squared_error

try:
    from xgboost import XGBRegressor
    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False

class MLModelHandler:
    def __init__(self):
        self.model_registry = {
            "Linear Regression": LinearRegression,
            "Random Forest": RandomForestRegressor,
            "Gradient Boosting": GradientBoostingRegressor,
            "SVR": SVR,
        }

        if HAS_XGBOOST:
            self.model_registry["XGBoost"] = XGBRegressor

    def get_model_list(self):
        return list(self.model_registry.keys())

    def train_and_predict(
        self,
        model_name: str,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_predict: pd.DataFrame,
    ):
        if model_name not in self.model_registry:
            raise ValueError(f"Model '{model_name}' is not supported")

        ModelClass = self.model_registry[model_name]

        if model_name == "XGBoost":
            model = ModelClass(
                n_estimators=200,
                max_depth=4,
                learning_rate=0.05,
                objective="reg:squarederror",
                random_state=42,
            )
        else:
            model = ModelClass()

        model.fit(X_train, y_train)
        preds = model.predict(X_predict)

        return preds, model

    def evaluate(self, y_true, y_pred):
        return {
            "mae": mean_absolute_error(y_true, y_pred),
            "rmse": np.sqrt(mean_squared_error(y_true, y_pred)),
        }
