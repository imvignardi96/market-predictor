import numpy as np
import logging
from itertools import combinations, chain
from sklearn.preprocessing import MinMaxScaler
from pandas import DataFrame


def create_sequences(data:DataFrame, lookback=60, forecast_horizon=2):
    sequences = []
    targets = []
    
    feature_data = data.drop(columns=['target']) # Excluir variable target
    target_data = data['target'].copy()
    logging.info(f'Features: {feature_data.columns}')

    for i in range(lookback, len(data) - forecast_horizon + 1):  # Prevent index out of range
        sequences.append(feature_data.iloc[i - lookback:i].values)  # Input sequence of past 'lookback' days
        targets.append(target_data.iloc[i:i + forecast_horizon].values)  # Predict next 2 days
    
    return np.array(sequences), np.array(targets)  # Targets will now have shape (samples, 2)

def obtain_split(X, y, training_scaler, validation_scaler) -> np.ndarray:
    # Calcular indices separacion
    train_split = int(training_scaler * len(X))  # Training set size
    val_split = int(validation_scaler * len(X))  # Validation set size

    # Dividir los datos
    X_train = X[:train_split-1]
    X_val = X[train_split:train_split+val_split-1]
    X_test = X[train_split+val_split:]

    y_train = y[:train_split-1]
    y_val = y[train_split:train_split+val_split-1]
    y_test = y[train_split+val_split:]

    return X_train, X_val, X_test, y_train, y_val, y_test

def scale_dataframe(scaler:MinMaxScaler, train_split:int, val_split:int, df:DataFrame, features:list):
    df_scaled = df.copy()
    
    # Get the actual DateTime indices for training, validation, and test splits
    train_index = df.index[:train_split]  # First part for training
    val_index = df.index[train_split:train_split+val_split]  # Validation set
    test_index = df.index[train_split+val_split:]  # Remaining test set

    # Fit the scaler on the training set only
    scaler.fit(df.loc[train_index, features])

    # Transform and reassign values
    df_scaled.loc[train_index, features] = scaler.transform(df.loc[train_index, features])
    df_scaled.loc[val_index, features] = scaler.transform(df.loc[val_index, features])
    df_scaled.loc[test_index, features] = scaler.transform(df.loc[test_index, features])
    
    return df_scaled, scaler


def generate_features(variable_features):
    # Generar solo dos combinaciones de features
    all_combinations = []
    for combo in combinations(variable_features, 2):
        # Unwrap tuples
        unwrapped = list(chain.from_iterable(c if isinstance(c, tuple) else [c] for c in combo))
        all_combinations.append(unwrapped)

    return all_combinations