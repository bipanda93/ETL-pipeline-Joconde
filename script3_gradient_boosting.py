import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.datasets import fetch_california_housing

# Chargement du dataset
housing = fetch_california_housing()
X = housing.data
Y = housing.target

print("Dimension de X :", X.shape)
print("Dimension de Y :", Y.shape)

df = pd.DataFrame(X, columns=housing.feature_names)
df["target"] = Y
print("\nAperçu du dataset :")
print(df.head(10))

# Split train/test
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

# Normalisation
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Modèle XGBoost
model = XGBRegressor(n_estimators=100, learning_rate=0.1, max_depth=6, random_state=42)
model.fit(X_train, Y_train)
Y_pred = model.predict(X_test)

# Métriques
print("\nMSE  :", round(mean_squared_error(Y_test, Y_pred), 4))
print("MAE  :", round(mean_absolute_error(Y_test, Y_pred), 4))
print("R²   :", round(r2_score(Y_test, Y_pred), 4))

# Test de plusieurs valeurs de learning_rate
print("\nTest de plusieurs valeurs de learning_rate :")
for lr in [0.01, 0.05, 0.1, 0.3]:
    model = XGBRegressor(n_estimators=100, learning_rate=lr, max_depth=6, random_state=42)
    model.fit(X_train, Y_train)
    y_pred = model.predict(X_test)
    r2 = r2_score(Y_test, y_pred)
    print("learning_rate =", lr, "-> R² =", round(r2, 4))