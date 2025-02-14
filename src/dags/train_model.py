from utils.sqlconnector import SQLConnector
import utils.modelmethods as mm
from airflow.decorators import task, dag
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException
from airflow.models import Variable
import pandas as pd
import pendulum
import logging

# Dag
@dag(
    dag_id='train_model',
    description='DAG para obtener los sentimientos de noticias',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
    schedule_interval='0 14 * * 6',  # A las 14:00 el sabado
    doc_md=
    """
        #### Documentacion Model Trainer.
    """
)
def train_model_dag():
    
    db_user = Variable.get('db_user')
    db_pwd = Variable.get('db_password')
                         
    connector = SQLConnector(username=db_user, password=db_pwd)
    
    @task(
        doc_md=
        """
            Esta tarea obtiene los tickers necesarios para el scraping.
            
            -**Returns**:
                -active_tickers: Lista de diccionarios con los tickers activos.
        """,
    )
    def get_tickers():
        # Fetch the tickers
        ticker_data = connector.read_data('tickers', {'active':1})
        active_tickers = ticker_data.to_dict(orient='records')

        return active_tickers
    
    @task(
        doc_md=
        """
            Esta tarea obtiene los datos a utilizar en la creacion del modelo.
            Genera un archivo temporal con los datos que puede utilizar el siguiente proceso.
            
            El uso del modulo temp_files tiene sus beneficios. Cuando Linux reinicie se eliminan
            si por cualquier casual quedaron pendientes de borrar.
            
            En este caso concreto queremos que se borren automaticamente.
        """
    )
    def get_data(ticker):
        import tempfile
        
        depth = int(Variable.get('model_data_depth'))

        if depth<=0:
            raise ValueError('Invalid data depth. Check Airflow variable "model_data_depth"')
        
        data_depth = pendulum.now().subtract(months=depth).date()
        logging.info(f'Meses de profundidad: {depth}')
        logging.info(f'Extraccion a partir de fecha: {data_depth}')
        
        ticker_id = ticker['id']
        ticker_code = ticker['ticker']
        
        # Obtenemos los datos de stock
        stock_data = connector.read_data('stock_data_daily', {'value_at':('>=', data_depth), 'ticker_id':ticker_id})
        logging.info(f'Datos extraidos')
        
        stock_data = stock_data[['value_at', 'opening_price', 'closing_price', 'volume', 'rsi', 
                                 'aroon_up', 'aroon_down', 'macd', 'macd_hist', 'macd_signal', 'obv']]
        
        # Generamos un fichero temporal para poder usarlo los datos en otro task especifico
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_file_path = temp_file.name
        stock_data.to_csv(temp_file_path, index=False)  # Save DataFrame as CSV
        temp_file.close()
        
        return {'code':ticker_code, 'path':temp_file_path}
    
    @task(
        doc_md=
        """
            Esta tarea lee el archivo temporal y genera el modelo
        """
    )
    def generate_models(ticker_dict):
        import keras
        from sklearn.preprocessing import MinMaxScaler
        from utils.plotter import LSTMPlotter
        import os
        
        try:
            # Extrer datos diccionario
            ticker_code = ticker_dict['code']
            file_path = ticker_dict['path']
            
            # Variables generadoras de configuracion base de modelo.
            batch_size = int(Variable.get('model_batch_size'))
            epochs = int(Variable.get('model_epochs'))
            lookback = int(Variable.get('model_lookback'))
            predict_days = int(Variable.get('model_predict_days'))
            patience = int(Variable.get('model_patience'))
            
            assert batch_size>0 and batch_size<=128
            assert epochs>0 and epochs<=300
            assert predict_days>0 and predict_days<=10
            assert lookback>0
            assert patience>1
            
            # Variables generadoras de capas
            initial_complexity = int(Variable.get('model_initial_complexity'))
            is_bidirectional = bool(int(Variable.get('model_bilateral')))
            max_layers = int(Variable.get('model_max_layers'))
            int_activation = Variable.get('model_int_activation')
            out_activation = Variable.get('model_out_activation')
            dropout_rate = float(Variable.get('model_dropout_rate'))
            
            assert initial_complexity>0 and initial_complexity<=128
            assert max_layers>0 and max_layers<=3
            assert dropout_rate>=0 and dropout_rate<=0.9
            
            # Variables de split de datos
            train_scaler = float(Variable.get('model_train_scaler'))
            validation_scaler = float(Variable.get('model_validation_scaler'))
            
            assert train_scaler+validation_scaler<=1 and validation_scaler>0 or train_scaler>0
            
            logging.info('Variables inicializadas')
            
            #############################################################
            ################### Tratamiento de datos ####################
            #############################################################
            
            stock_data = pd.read_csv(file_path)
            
            logging.info(f'Datos leidos del archivo temporal: {file_path}')
            
            stock_data.set_index('value_at', inplace=True)
            stock_data['target'] = stock_data['closing_price'] # Clonar columna. Esta se utilizara para y
            stock_data.dropna(inplace=True)
            
            logging.info(f'Pretratamiento realizado')
            
            #############################################################
            ######### A partir de aqui comienzan las iteraciones ########
            #############################################################
            
            # Features a utilizar. Se ha definido un minimo de 3 features y un maximo de 4.
            # El valor de macd es redundante
            variable_columns = ['opening_price', 'obv', ('aroon_up', 'aroon_down'), 'macd_hist', 'rsi']
            base_columns = ['closing_price', 'target']
            
            combination_columns = mm.generate_features(variable_columns)
            
            logging.info(f'Combinacion de features generada')
            
            complexities = [initial_complexity // (2**i) for i in range(initial_complexity)]
            
            logging.info(f'Combinacion de complejidades generada')
            
            count=1
            
            plotter = LSTMPlotter(rows=len(combination_columns), cols=len(complexities))
            
            for combination in combination_columns:                
                # Obtenemos las features de la iteracion
                features = base_columns+combination            
            
                df = stock_data[features].copy()  # Ensure df is a separate copy
                
                train_split = int(train_scaler * len(df))  # Training set size
                val_split = int(validation_scaler * len(df))  # Validation set size
                
                scaler = MinMaxScaler(feature_range=(0, 1))

                df_scaled = mm.scale_dataframe(scaler, train_split, val_split, df, features)
                
                logging.info(f'Dataframe escalado')
                
                X, y = mm.create_sequences(df_scaled, lookback, predict_days)
                
                logging.info(f'Secuencias creadas')
                
                # Obtenemos los splits
                X_train, X_val, X_test, y_train, y_val, y_test = mm.obtain_split(X, y, train_scaler, validation_scaler)
                
                logging.info('Datos listos')
                
                ####################################################
                ############## Generador de modelos ################
                ####################################################
                for n_layers in range(1, complexities):
                    to_use = complexities[:n_layers]
                    
                    logging.info(f'Generando modelo {count}, complejidades {to_use} y features {features} para el ticker {ticker_code}')
                    
                    model = keras.Sequential()
                    model.add(keras.layers.InputLayer(input_shape=(X_train.shape[1], X_train.shape[2])))
                    for units in to_use:
                        if is_bidirectional:
                            logging.info(f'Incluyendo capa BiLSTM {int_activation} de complejidad {units}')
                            model.add(keras.layers.Bidirectional(keras.layers.LSTM(units=units, activation=int_activation)))
                        else:
                            logging.info(f'Incluyendo capa LSTM {int_activation} de complejidad {units}')
                            model.add(keras.layers.LSTM(units, activation=int_activation))
                    
                    if dropout_rate!=0:
                        logging.info(f'Incluyendo capa Dropout con proporcion {dropout_rate}')
                        model.add(keras.layers.Dropout(dropout_rate))
                        
                    logging.info(f'Incluyendo capa de salida {out_activation} con {predict_days} unidades')
                    model.add(keras.layers.Dense(units=predict_days, activation=out_activation))
                    
                    logging.info(f'Compilando modelo con optimizador adam, funcion de perdida mse')
                    model.compile(optimizer='adam', loss='mape', metrics=['mse', 'mae'])

                    cp_path = f"model_{ticker_code.lower()}_{'_'.join(str(feature) for feature in features)}_{n_layers}.keras"
                    if os.path.exists(cp_path):
                        os.remove(cp_path)
                        
                    cp = keras.callbacks.ModelCheckpoint(cp_path, save_best_only=True, save_weights_only=False)
                    model.compile(optimizer='adam', loss='mape', metrics=['mse', 'mape'])
                    early_stopping = keras.callbacks.EarlyStopping(monitor='val_loss', patience=patience, restore_best_weights=True)
                    
                    # Hacer fit del modelo actual
                    model.fit(X_train, y_train, validation_data=(X_val, y_val), epochs=epochs, batch_size=batch_size, callbacks=[cp,early_stopping])

                    # Carga mejor modelo
                    model.load_weights(cp_path)
                    
                    # Evaluacion del modelo
                    test_loss = model.evaluate(X_test, y_test)
                    logging.info(f'Test Loss: {test_loss}')
                    
                    logging.info(f'Modelo completado')
                    
                    # Predicciones del modelo
                    y_pred = model.predict(X_test)        
                    
                    plotter.add_plot(
                        y_test=y_test,
                        y_pred=y_pred,
                        model_name=cp_path
                    ) 
                    
                    count+=1
                       
                    
        except AssertionError as e:
            logging.error(f'Una o mas variables tienen un valor erroneo: {str(e)}')
            raise AirflowFailException

    tickers = get_tickers()
    dictionary = get_data.expand(ticker=tickers)
    generate_models.expand(ticker_dict=dictionary)
    
model_instance = train_model_dag()