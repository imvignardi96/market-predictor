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

    for i in range(lookback, len(data) - forecast_horizon + 1):
        sequences.append(feature_data.iloc[i - lookback:i].values)  # Sequencias con una profundidad de N dias
        targets.append(target_data.iloc[i:i + forecast_horizon].values)  # Predecir N dias
    
    return np.array(sequences), np.array(targets) 

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
    
    # Se obtienen los indices sobre los cuales hacer el fit y/o transformaciones
    train_index = df.index[:train_split]  # Parte de entrenamiento
    val_index = df.index[train_split:train_split+val_split]  # Parte de validacion
    test_index = df.index[train_split+val_split:]  # Parte de test

    # Hacer fit del escalador sobre datos de entrenamiento
    scaler.fit(df.loc[train_index, features])

    # Transformar los datos
    df_scaled.loc[train_index, features] = scaler.transform(df.loc[train_index, features]) # Entrenamiento
    df_scaled.loc[val_index, features] = scaler.transform(df.loc[val_index, features]) # Validacion
    df_scaled.loc[test_index, features] = scaler.transform(df.loc[test_index, features]) # Test
    
    return df_scaled, scaler


def generate_features(variable_features, max_combination):
    # Generar solo dos combinaciones de features
    all_combinations = []
    for combo in combinations(variable_features, max_combination):
        # Unwrap tuples
        unwrapped = list(chain.from_iterable(c if isinstance(c, tuple) else [c] for c in combo))
        all_combinations.append(unwrapped)

    return all_combinations