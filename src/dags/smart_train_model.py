from utils.sqlconnector import SQLConnector
import utils.modelmethods as mm
from airflow.decorators import task, dag
from airflow.models.variable import Variable
from airflow.exceptions import AirflowFailException
import pandas as pd
import pendulum
import logging


# Dag
@dag(
    dag_id='smart_train_model',
    description='DAG para el entrenamiento de modelos inteligentes',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=3,
    max_active_runs=1,
    schedule='0 14 * * 6',  # A las 14:00 el sabado
    doc_md=
    """
        #### Documentacion Entrenamiento de Modelos BiLSTM/LSTM.

        El DAG se encarga de entrenar múltiples modelos BiLSTM/LSTM en paralelo, permitiendo visualizar rápidamente diversos resultados y facilitando la selección del modelo más adecuado para el usuario.

        ##### Configuración de Variables

        Las variables de Airflow que comienzan con `model_` en la sección de variables controlan las opciones de entrenamiento de este DAG. Entre ellas destacan:

        - **Complejidad máxima de las capas**  
        - **Número máximo de capas**  
        - **Tamaño del batch**  
        - **Otros hiperparámetros ajustables**  

        La mayoría de estos parámetros pueden configurarse desde la interfaz gráfica (GUI), incluyendo la opción de activar o desactivar una capa de *dropout*.

        ##### Flujo de Actividades

        Las actividades que conforman el DAG son las siguientes:

        1. **Lectura de tickers**  
        Obtiene los datos de las empresas marcadas como activas en la base de datos.  

        2. **Obtención de datos**  
        Recupera la información desde la base de datos MySQL, con un volumen configurable de datos.  

        3. **Generación de modelos**  
        Construye los modelos en función de las variables definidas.  

        4. **Cálculo de métricas y visualización**  
        Extrae las principales métricas de rendimiento para facilitar la selección del mejor modelo.  

        5. **Envío de resultados**  
        Envía un correo con los modelos más destacados en cada métrica y sus respectivos resultados.  

    """
)
def train_model_dag():
    # Variables de base de datos
    db_user = Variable.get('db_user')
    db_pwd = Variable.get('db_password')
                         
    connector = SQLConnector(username=db_user, password=db_pwd)
    
    @task(
        doc_md=
        """
            Esta tarea obtiene los tickers necesarios para el entrenamiento.
            
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
            
            En este caso concreto queremos que se borren en el reinicio del sistema pero no automaticamente.
            Si se borrasen automaticamente, al cambiar de task el archivo se cerraria y se perderia.
            
            -**Args**:
                -ticker: Diccionario que contiene el id del ticker y su nombre. Se utiliza para saber que datos extraer
            
            -**Returns**:
                -ticker_dict: Diccionnario con nombre de ticker y su archivo asociado.
        """
    )
    def get_data(ticker):
        import tempfile
        
        # Retornamos el numero de meses de datos que se van a extraer de MySQL
        depth = int(Variable.get('model_data_depth'))

        if depth<1:
            raise ValueError('Profunidad de datos invalida. Compruebe "model_data_depth"')
        
        # Obtenemos la fecha en concreto de hace "depth" meses
        data_depth = pendulum.now().subtract(months=depth).date()
        logging.info(f'Meses de profundidad: {depth}')
        logging.info(f'Extraccion a partir de fecha: {data_depth}')
        
        ticker_id = ticker['id']
        ticker_code = ticker['ticker']
        
        # Obtenemos los datos de stock
        stock_data = connector.read_data('stock_data_daily_w_news', {'value_at':('>=', data_depth), 'ticker_id':ticker_id})
        logging.info('Datos extraidos')
        
        # Generamos un fichero temporal para poder usarlo los datos en otro task especifico
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        temp_file_path = temp_file.name
        stock_data.to_csv(temp_file_path, index=False)  # Save DataFrame as CSV
        temp_file.close()
        
        return {'code':ticker_code, 'path':temp_file_path}
    
    @task(
        doc_md=
        """
            Esta tarea lee el archivo temporal generado con NamedTemporaryFile y crea todos los modelos.
            
            La actividad es dependiente de numerosas variables que se definen en la interfaz de Airflow. 
            Por ello el control de los modelos generados es dependiente del conocimiento de los hyperparametros
            que se pueden aplicar sobre un modelo de redes neuronales.
            
            -**Args**:
                - ticker_dict: Diccionnario con nombre de ticker y el archivo que contiene sus datos.       
        """
    )
    def generate_models(ticker_dict):
        import os
        import keras
        import keras_tuner as kt
        from sklearn.preprocessing import MinMaxScaler
        import numpy as np
        import joblib
        import warnings
        
        from hypermodels.stocks import Smart
        
        warnings.filterwarnings('ignore')
        
        try:
            # Extrer datos diccionario
            ticker_code = ticker_dict['code']
            file_path = ticker_dict['path']
            
            # Variables generadoras de configuracion base de modelo.
            predict_days = int(Variable.get('model_predict_days'))
            patience = int(Variable.get('model_patience'))
            lookback = int(Variable.get('model_lookback'))
            
            assert predict_days>0 and predict_days<=10
            assert patience>1

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
            stock_data.sort_index(inplace=True, ascending=True)
            stock_data['target'] = stock_data['closing_price'] # Clonar columna. Esta se utilizara para y
            
            logging.info('Pretratamiento realizado')
            
            #############################################################
            ######### A partir de aqui comienzan las iteraciones ########
            #############################################################
            
            # Features a utilizar. Se ha definido un minimo de 3 features y un maximo de 4.
            # El valor de macd es redundante, macd_hist contiene la informacion necesaria
            features = Variable.get("model_use_features")
            
            logging.info('Combinacion de features generada')
                      
        
            df = stock_data[features].copy()  # Obtenemos el df con las features que queremos
            df.dropna(inplace=True) # Ahora se eliminan las filas con Nan
            
            train_split = int(train_scaler * len(df))  # Training set size
            val_split = int(validation_scaler * len(df))  # Validation set size
            
            scaler = MinMaxScaler(feature_range=(0, 1))

            # Escalamos el dataframe y retornamos el scaler conn el fit para guardarlo posteriormente
            df_scaled, scaler = mm.scale_dataframe(scaler, train_split, val_split, df, features)
            
            logging.info(f'Dataframe escalado: {df_scaled.iloc[1].values}')
            
            # Creamos las secuencias en funcion del numero de dias que queremos utilizar para predecir
            X, y = mm.create_sequences(df_scaled, lookback, predict_days)
            
            logging.info(f'Secuencias creadas: {X[0]}')
            
            # Obtenemos los splits
            X_train, X_val, X_test, y_train, y_val, y_test = mm.obtain_split(X, y, train_scaler, validation_scaler)
            
            logging.info(f'Datos listos: {X_train[0]}')
            logging.info(f'Forma datos test: {X_test.shape}')
            
            ####################################################
            ############## Generador de modelos ################
            ####################################################                
            logging.info('Generando modelo')
            
            # Prepare model builder function with context
            input_shape = (X_train.shape[1], X_train.shape[2])
            model_builder = Smart(input_shape)

            tuner = kt.RandomSearch(
                model_builder,
                objective='val_loss',
                max_trials=10,
                executions_per_trial=1,
                directory=os.path.join(Variable.get('model_path'), 'tuning'),
                project_name=f'{ticker_code.lower()}_{"_".join(features)}'
            )
            
            logging.info('Tuner creado')
            
            # Set up callbacks
            base_path = Variable.get('model_path')
            this_model = os.path.join(base_path, f'model_{ticker_code.lower()}')
            
            # Create directories if not exist
            os.makedirs(this_model, exist_ok=True)

            logging.info('Path disponible')
            
            # Model checkpoint and early stopping
            early_stopping = keras.callbacks.EarlyStopping(monitor='val_loss', patience=patience, restore_best_weights=True)
            
            logging.info('Checkpoint y early stopper creados')
            
            # Perform hyperparameter search with early stopping and model checkpoint
            tuner.search(
                X_train, y_train,
                validation_data=(X_val, y_val),
                callbacks=[early_stopping],
                verbose=2
            )
            
            logging.info('Busqueda finalizada')
            
            best_models = tuner.get_best_models(num_models=5)
            # best_hps = tuner.get_best_hyperparameters(num_trials=5)
            
            for idx, best_model in enumerate(best_models,1):
                logging.info('Mejor modelo seleccionado')

                bm_filename = f"model_{idx}_{ticker_code.lower()}_{'_'.join(str(feature) for feature in features)}.keras"
                model_file = os.path.join(this_model, bm_filename)
                best_model.save(model_file)
                
            # Guardar los datos de test en la misma carpeta con la misma nomenclatura
            x_test_filename = f"x_test_{ticker_code.lower()}_{'_'.join(str(feature) for feature in features)}.npy"
            x_path = os.path.join(this_model, x_test_filename)
            np.save(x_path, X_test)
            
            y_test_filename = f"y_test_{ticker_code.lower()}_{'_'.join(str(feature) for feature in features)}.npy"
            y_path = os.path.join(this_model, y_test_filename)
            np.save(y_path, y_test)
            
            # Guardar el minmax scaler
            scaler_filename = f"scaler_{ticker_code.lower()}_{'_'.join(str(feature) for feature in features)}.pkl"
            scaler_path = os.path.join(this_model, scaler_filename)
            joblib.dump(scaler, scaler_path)
                                                    
        except AssertionError as e:
            logging.error(f'Una o mas variables tienen un valor erroneo: {str(e)}')
            raise AirflowFailException
        
    @task(
        doc_md=
        """
            Esta tarea genera las predicciones y computa algunas de las metricas de mayor valor en
            la prediccion de series temporales.
            
            Para poder llevarlo acabo se lee de forma secuencial todas las carpetas de los modelos.
            Cada una de ellas contiene los datos necesarios para poder computar las predicciones y graficar los
            resultados correctamente.
            
            Requiere: Un scaler, un subconjunto X_test, un subconjunto y_test y un modelo .keras
        """,
        trigger_rule='all_done'
    )
    def generate_predictions():
        import keras
        import numpy as np
        import joblib
        from sklearn.preprocessing import MinMaxScaler
        from utils.plotter import LSTMPlotter
        import os
        
        # Obtener todos los subdirectorios. Cada uno contiene un modelo
        base_path = Variable.get('model_path')
        best_models = {}
        directories = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]
        plotter = LSTMPlotter()
        
        # Inicializacion de variables sobre un valor absurdo.
        curr_mape=100000
        curr_dir=-100000
        curr_r2=-100000
        curr_mse=100000
        
        # Se itera cada directorio. En este debe existir un modelo, un scaler, X_test e y_test.
        # Si no existe se pasa al siguiente modelo por falta de datos.
        for directory in directories:
            logging.info(f'Inspeccionando directorio {os.path.join(base_path, directory)}')
            
            # Se obtiene cada archivo por separado
            model_files = [file for file in os.listdir(os.path.join(base_path, directory)) if file.endswith('.keras')]
            
            for model_file in model_files:
                X_file = [file for file in os.listdir(os.path.join(base_path, directory)) if file.startswith('x_test')]
                y_file = [file for file in os.listdir(os.path.join(base_path, directory)) if file.startswith('y_test')]
                scaler_file = [file for file in os.listdir(os.path.join(base_path, directory)) if file.startswith('scaler')]
                
                # Si se encuentran todos los archivos se procesa
                if X_file and y_file and scaler_file:
                    try:
                        model_file_path = os.path.join(base_path, directory, model_file)
                        X_file_path = os.path.join(base_path, directory, X_file[0])
                        y_file_path = os.path.join(base_path, directory, y_file[0])
                        scaler_file_path = os.path.join(base_path, directory, scaler_file[0])

                        
                        # Carga mejor modelo
                        model:keras.Sequential = keras.models.load_model(model_file_path)
                        X_test = np.load(X_file_path)
                        y_test = np.load(y_file_path)
                        fitted_scaler:MinMaxScaler = joblib.load(scaler_file_path)
                        
                        # Predicciones del modelo
                        y_pred = model.predict(X_test, verbose=0)    
                        
                        logging.info(f"Forma inicial de y: {y_test.shape}")
                        logging.info(f"Forma del scaler: {fitted_scaler.data_max_.shape}")
                        
                        # Se obtiene el numero de columnas que haran falta para poder usar el scaler nuevamente
                        n_zero_cols = fitted_scaler.scale_.shape[0] - y_test.shape[1]
                        
                        # Invertir transformacion agregando columnas con ceros
                        zeros = np.zeros((y_test.shape[0], n_zero_cols))
                        y_test_expanded = np.hstack((y_test, zeros))
                        y_pred_expanded = np.hstack((y_pred, zeros))
                        
                        logging.info(f"Nueva forma de y: {y_test_expanded.shape}")
                        
                        # Obtencion de los valores reales
                        y_test_real = fitted_scaler.inverse_transform(y_test_expanded)[:, 0]
                        y_pred_real = fitted_scaler.inverse_transform(y_pred_expanded)[:, 0]
                        
                        logging.info('Predicciones realizadas')    
                        
                        # Graficado y obtencion de las metricas principales
                        mape, direccional, r2, mse = plotter.add_plot(
                            y_test=y_test_real,
                            y_pred=y_pred_real,
                            model_path=os.path.join(base_path, directory, f'{model_file}.png')
                        ) 
                        
                        # Cerramos la figura para evitar consumo de memoria.
                        plotter.close_figure()
                        
                        # Almacenamos el mejor modelo de cada metrica
                        if mape<curr_mape:
                            curr_mape=mape
                            best_models['mape']={"file":model_file, "result":mape}
                        if direccional>curr_dir:
                            curr_dir=direccional
                            best_models['da']={"file":model_file, "result":direccional}
                        if r2>curr_r2:
                            curr_r2=r2
                            best_models['r2']={"file":model_file, "result":r2}
                        if mse<curr_mse:
                            curr_mse=mse
                            best_models['mse']={"file":model_file, "result":mse}
                            
                    except Exception as e:
                        logging.error(f"Error en carga de modelo o creacion de predicciones: {e}")
                        raise AirflowFailException                
                else:
                    logging.warning('Archivos para modelo no encontrados')
                
        return best_models
                
    @task(
        doc_md="""Task para envio de email con resultados""",
        trigger_rule = "all_done"
    )
    def send_email(best_models):
        from airflow.providers.smtp.operators.smtp import EmailOperator
        import zipfile
        import json
        import os
        
        # Se obtiene la lista de destinatarios y se define el path al zip que almacenara los graficos
        base_path = Variable.get('model_path')
        destinataries = json.loads(Variable.get('model_destinataries'))['destinataries']
        zip_file = os.path.join(base_path, 'lstm_outputs.zip')
        
        # Generar zip
        count = 0
        logging.info('Generando archivo zip con resultados')
        with zipfile.ZipFile(zip_file, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(base_path):
                for file in files:
                    if file.lower().endswith('.png'):
                        file_path = os.path.join(root, file)
                        archive_name = os.path.relpath(file_path, base_path)  # Preserve relative paths
                        zf.write(file_path, arcname=archive_name)
                        count+=1

        # Leer html
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        contents_folder = os.path.abspath(os.path.join(dag_folder, "..", "contents"))
        
        # Si algun archivo se incluyo enn el zip se manda un mennsaje de exito, aunque fuese parcial.
        if count!=0:
            logging.info('Zip creado')
            send_file = [zip_file]
            html_file_path = os.path.join(contents_folder, "body.html")

            with open(html_file_path, "r", encoding="utf-8") as file:
                html_content = file.read()
                
                # Se muta el texto para incluir los mejores modelos en cada metrica
                html_content = html_content.replace(
                    "{change_me1}",
                    f"""Se ha detectado que los mejores modelos son:
                    <ul>
                        <li><b>MAPE:</b> {best_models['mape']['file']} con resultado de {float(best_models['mape']['result']):.2f}%</li>
                        <li><b>DA:</b> {best_models['da']['file']} con resultado de {float(best_models['da']['result']):.2f}%</li>
                        <li><b>R2:</b> {best_models['r2']['file']} con resultado de {float(best_models['r2']['result']):.4f}</li>
                        <li><b>MSE:</b> {best_models['mse']['file']} con resultado de {float(best_models['mse']['result']):.4f}</li>
                    </ul>""")
        # Si no se inncluyo ningun archivo se manda un email de error.
        else:
            logging.error('No se encontraron resultados')
            send_file = None
            html_file_path = os.path.join(contents_folder, "body_error.html")

            with open(html_file_path, "r", encoding="utf-8") as file:
                html_content = file.read()

        logging.info('Enviando email')
        email = EmailOperator(
            task_id='lstm_results',
            to=destinataries,
            subject=f"Resultados LSTM {pendulum.now().strftime('%Y-%m-%d')}",
            html_content=html_content,
            files=send_file
        )
        return email.execute({})
                    
    # Se define el orden de las actividades
    tickers = get_tickers()
    dictionary = get_data.expand(ticker=tickers)
    model_gen = generate_models.expand(ticker_dict=dictionary)
    # predictions_gen = generate_predictions()
    # send_email(predictions_gen)
    
    # model_gen >> predictions_gen

# Se instancia el dag
model_instance = train_model_dag()