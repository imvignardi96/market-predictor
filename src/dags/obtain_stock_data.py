from utils.ibconnector import IBApi
import pendulum
from airflow.decorators import task, dag
from airflow.models import Variable

from utils.sqlconnector import SQLConnector

import pandas as pd

import logging 
import time
    
@dag(
    dag_id='stock_data_extraction',
    description='DAG para obtener datos de la plataforma TWS de IB',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=3,
    max_active_runs=1,
    schedule_interval='30 23 * * *',  # At 23:30
    doc_md=
    """
        #### Documentacion Extractor datos stocks.
        
        El presente DAG se encarga de obtener periodicamente los valores de apertura, cierre, maximo, minimo y volumen de la API de Interactive Brokers.
        Para que funcione correctamente, el software IB TWS (Trader Workstation) debe estar ejecutandose. Asegurar su funcionamiento es parte de otro DAG.
        Para ingestar se utiliza un usuario propio de procesamiento completamente diferente al que utiliza airflow para su base de datos propia.
        
        Los pasos que realiza son:
        1) Obtencion de los Tickers activos. Los tickers son los simbolos que representan a una empresa en el mercado de valores.
        2) Obtencion de los datos de stock. Si la fecha minima en la BBDD es superior a la definida en la variable de Airflow 'data_start', se ejecuta hasta obtener los datos actualizados.
        3) Calculo de indicadores tecnicos. Algunos indicadores como el Aroon o el RSI se obtienen previa ingesta en la BBDD.
        4) Ingesta en base de datos. La base de datos esta preparada para no insertar datos repetidos. 
    """
)
def stock_data_dag():
    db_user = Variable.get('db_user')
    db_pwd = Variable.get('db_password')
                         
    connector = SQLConnector(username=db_user, password=db_pwd)
    
    @task(
        doc_md=
        """
            Esta tarea obtiene los tickers necesarios para el scraping.
            
            -**Returns**:
                -active_tickers: Lista de diccionarios con los tickers activos.
        """
    )
    def get_tickers():
        # Fetch the tickers
        ticker_data = connector.read_data('tickers', {'active':1})
        active_tickers = ticker_data.to_dict(orient='records')

        return active_tickers
    
    @task(
        doc_md=
        """
            Esta tarea obtiene los datos para cada ticker.
            
            -**Returns**:
                -active_tickers: Lista de diccionarios con los tickers activos.
        """
    )
    def get_data(ticker:dict, **kwargs):
        from ibapi.contract import Contract
        
        idx = int(kwargs['ti'].map_index)
        
        ib_host = Variable.get('ib_host')
        ib_port = int(Variable.get('ib_port'))
        ib_client = int(Variable.get('ib_client'))+idx

        app = IBApi()

        app.connect_ib(ib_host, ib_port, ib_client)
        
        # Obtener los valores de las columnas
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        contract = Contract()
        contract.symbol = ticker_code
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        # Primero debemos obtener la fecha minima que se enncuentra en la bbdd para el ticker
        query = f"SELECT MAX(value_at) FROM stock_data_daily WHERE ticker_id={ticker_id}"
        max_date = connector.custom_query(query)
        
        max_date_value = str(max_date.iloc[0, 0])
        
        logging.info(f"Fecha maxima de {ticker_code}: {max_date_value}")
        
        start_date = pendulum.now(tz='UTC').date()
        
        if max_date_value is None:
            end_date = pendulum.from_format(Variable.get('data_start'), 'YYYY-MM-DD', tz='UTC').date()
            n_points = '1 W'
        else:
            end_date = pendulum.from_format(max_date_value, 'YYYY-MM-DD', tz='UTC').date()

        # Inicializacion de variables necesarias. el ID no puede ser UUID, debe ser un entero.
        execution_date = start_date.strftime('%Y%m%d-%H:%M:%S')
        count = 0
        req_id = f"{ticker_id}{count}"
        ib_granularity = Variable.get('ib_granularity')
        
        logging.info("Parametros establecidos.")

        # Bucle para obtener datos todas las fechas
        first_exec = True
        while start_date > end_date:
            time.sleep(1)
            # Esperar a evento activo
            if app.data_ready_event.is_set():                
                diff = start_date.diff(end_date).in_days()
                if diff>=7:
                    if not first_exec:
                        start_date = start_date-pendulum.duration(weeks=1)
                        n_points = '1 W'
                    else:
                        first_exec = False
                else:
                    start_date = start_date-pendulum.duration(days=diff)
                    n_points = f'{diff} D'
                    
                logging.info(f"Obteniendo datos de {start_date} con profundidad {n_points}")
                logging.info(f"Id del request: {req_id}")
                
                app.data_ready_event.clear()
                
                app.reqHistoricalData(req_id, contract, execution_date, f"{n_points}", f"{ib_granularity}", "TRADES", 1, 1, False, [])
                    
                execution_date = start_date.strftime('%Y%m%d-%H:%M:%S')
                
                count += 1
                req_id = f"{ticker_id}{count}"

        app.disconnect()
        
        # Datos historiccos a dataframe
        df = pd.DataFrame(app.historical_data)
        
        logging.info(f"Dataframe creado")

        df['value_at'] = pd.to_datetime(df['value_at'].astype(str), format='%Y%m%d', errors='coerce')
        df['ticker_id'] = ticker_id
        
        # Ingesta en BBDD
        list_of_data = df.to_dict(orient='records')
        connector.insert_data('stock_data_daily', list_of_data, 'IGNORE')
        
    @task(
        doc_md=
        """
            Esta tarea obtiene ciertos indicadores para cada ticker.
        """
    )
    def process_data(ticker:dict, **kwargs):
        from utils.compute import technicalIndicators
        
        # Parametros indicadores tecnicos
        rsi_period:int = Variable.get('cp_rsi_period')
        aroon_period:int = Variable.get('cp_aroon_period')
        macd_fast:int = Variable.get('cp_macd_fast')
        macd_slow:int = Variable.get('cp_macd_slow')
        macd_signal:int = Variable.get('cp_macd_signal')
        
        # Inicializamos la clase
        indicators = technicalIndicators(
            rsi_period,
            aroon_period,
            macd_fast,
            macd_slow,
            macd_signal
        )
        
        # Obtener los valores de las columnas
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        # Obtener el mayor periodo para conocer la profundidad de los datos a extraer
        max_depth = max([rsi_period, aroon_period, macd_fast, macd_slow, macd_signal])
        
        query = f"""
            SELECT * FROM (
                SELECT 
                    *,
                    MIN(value_at) AS START_DATE
                FROM stock_data_daily 
                WHERE ticker_id={ticker_id}
                    AND (rsi IS NULL OR aroon_up IS NULL OR macd IS NULL or obv IS NULL)
                ORDER BY value_at DESC
            ) AS A
            WHERE value_at >= DATE_SUB(START_DATE, INTERVAL {max_depth} DAY);
        """
        data = connector.custom_query(query)

        logging.info(f"Datos a computar obtenidos")
        
        logging.info(data)
        logging.info(data.columns)
        
        
        
    tickers = get_tickers()
    get_prices = get_data.expand(ticker=tickers)
    get_indicators = process_data.expand(ticker=tickers)
    
    tickers >> get_prices >> get_indicators

# Create the DAG instance
stock_data_instance = stock_data_dag()
