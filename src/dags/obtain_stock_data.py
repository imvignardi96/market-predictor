from utils.ibconnector import IBApi
import pendulum
from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException, AirflowFailException

from utils.sqlconnector import SQLConnector

import pandas as pd

import logging 
import time
    
@dag(
    dag_id='stock_data_extraction',
    description='DAG para obtener datos de la plataforma TWS de IB',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=5,
    max_active_runs=1,
    schedule_interval='30 23 * * *',  # At 23:30
    doc_md=
    """
        #### Documentacion Extractor datos stocks.
        
        El presente DAG se encarga de obtener periodicamente los valores de apertura, cierre, maximo, minimo y volumen de la API de Interactive Brokers.
        Para que funcione correctamente, el software IB TWS (Trader Workstation o Gateway) debe estar ejecutandose. Asegurar su funcionamiento es parte del usuario.
        Para ingestar se utiliza un usuario propio de procesamiento completamente diferente al que utiliza airflow para su base de datos propia.
        
        Es importante mencionar que la API de IB es asincrona. Luego se debe dar uso de un thread.
        
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
        """,
    )
    def get_tickers():
        # Obtener los tickers
        ticker_data = connector.read_data('tickers', {'active':1})
        active_tickers = ticker_data.to_dict(orient='records')

        return active_tickers
    
    @task(
        doc_md=
        """
            Esta tarea obtiene los datos para cada ticker.
            
            -**Returns**:
                -active_tickers: Lista de diccionarios con los tickers activos.
        """,
        trigger_rule='all_success'
    )
    def get_data(ticker:dict, **kwargs):
        from ibapi.contract import Contract
        
        idx = int(kwargs['ti'].map_index)
        
        ib_host = Variable.get('ib_host')
        ib_port = int(Variable.get('ib_port'))
        ib_client = int(Variable.get('ib_client'))+idx
        ib_granularity = Variable.get('ib_granularity')

        app = IBApi()

        app.connect_ib(ib_host, ib_port, ib_client)
        
        # Obtener los valores de las columnas
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        # Por defecto siempre se van a extraer contratos Smart (Acciones) en dolares. Esta moneda es la mas utilizada mundialmente
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
        
        # Obtenemos la fecha actual para calcular la diferenncia de dias entre hoy y el ultimo dato
        start_date = pendulum.now(tz='UTC').date()
        
        # Si la query no retorno una fecha, entonces se va a utilizar la fecha minima definida en Airflow.
        if max_date_value in [None, 'None', '', 'nan', 'NaT'] or (isinstance(max_date_value, float) and pd.isna(max_date_value)):
            data_start = Variable.get('cp_data_start')
            logging.info(f'Fecha a utilizar: {data_start}')
            end_date = pendulum.from_format(data_start, 'YYYY-MM-DD', tz='UTC').date()
            n_points = '1 Y'
        # Si encontro fecha minima se obtienen los dias de diferencia
        else:
            end_date = pendulum.from_format(max_date_value, 'YYYY-MM-DD', tz='UTC').date()
            diff = start_date.diff(end_date).in_days()
            n_points = f'{diff} D'

        # Inicializacion de variables necesarias. el ID no puede ser UUID, debe ser un entero.
        execution_date = start_date.strftime('%Y%m%d-%H:%M:%S')
        count = 0
        
        logging.info("Parametros establecidos.")

        # Bucle para obtener datos todas las fechas
        first_exec = True
        while start_date > end_date:
            time.sleep(1)
            # Esperar a evento activo
            if app.data_ready_event.is_set():
                # Diferencia de dias                
                diff = start_date.diff(end_date).in_days()
                count += 1
                req_id = f"{ticker_id}{count}"
                # Si la diferencia es superior a un ano. Obtener 1 Y de datos.
                if diff>=366:
                    if not first_exec:
                        start_date = start_date-pendulum.duration(years=1)
                        n_points = '1 Y'
                    else:
                        first_exec = False
                # Si es inferior obtener el numero de dias para finalizar
                else:
                    start_date = start_date-pendulum.duration(days=diff)
                    n_points = f'{diff} D'
                    
                logging.info(f"Obteniendo datos de {start_date} con profundidad {n_points}")
                logging.info(f"Id del request: {req_id}")
                
                # Limpiar el flag de datos listos (thread)
                app.data_ready_event.clear()
                
                # Hacer request de los datos historicos.
                app.reqHistoricalData(req_id, contract, execution_date, f"{n_points}", f"{ib_granularity}", "TRADES", 1, 1, False, [])
                    
                execution_date = start_date.strftime('%Y%m%d-%H:%M:%S')
            # Handling errores
            if app.error_code==162:
                break
            elif app.error_code is not None:
                app.disconnect()
                raise AirflowFailException

        time.sleep(10)
        app.disconnect()
        
        # Datos historicos a dataframe
        df = pd.DataFrame(app.historical_data)
        
        # Si no esta vacio se ingestan los datos en base de datos ordenados por fecha.
        if df is not None and not df.empty:
            logging.info(f"Dataframe creado")

            df['value_at'] = pd.to_datetime(df['value_at'].astype(str), format='%Y%m%d', errors='coerce')
            df['ticker_id'] = ticker_id
            df.sort_index(ascending=True, inplace=True)
            
            # Ingesta en BBDD
            list_of_data = df.to_dict(orient='records')
            connector.insert_data('stock_data_daily', list_of_data, 'IGNORE')
        else:
            logging.info(f"Dataframe vacio. Fin de semana no se generan datos.")
            raise AirflowSkipException
        
    @task(
        doc_md=
        """
            Esta tarea obtiene ciertos indicadores para cada ticker.
        """,
        trigger_rule='all_done'
    )
    def process_data(ticker:dict, **kwargs):
        from utils.compute import technicalIndicators
        
        # Parametros indicadores tecnicos
        rsi_period = int(Variable.get('cp_rsi_period'))
        aroon_period = int(Variable.get('cp_aroon_period'))
        macd_fast = int(Variable.get('cp_macd_fast'))
        macd_slow = int(Variable.get('cp_macd_slow'))
        macd_signal = int(Variable.get('cp_macd_signal'))
        adx_period = int(Variable.get('cp_adx_period'))
        atr_period = int(Variable.get('cp_atr_period'))
        data_depth = int(Variable.get('model_data_depth')) # Se computaran los datos que se utilizaran en el modelo
        
        # Inicializamos la clase
        indicators = technicalIndicators(
            rsi_period,
            aroon_period,
            macd_fast,
            macd_slow,
            macd_signal,
            adx_period,
            atr_period
        )
        
        # Obtener los valores de las columnas
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        # Obtener siempre el numero maximo de datos a utilizar para computar los indicadores tecnicos
        query = f"""
            SELECT 
                *
            FROM stock_data_daily
            WHERE ticker_id = {ticker_id}
                AND (rsi IS NULL OR aroon_up IS NULL OR macd IS NULL OR obv IS NULL OR atr IS NULL or adx IS NULL)
                AND value_at >= (
                    SELECT DATE_SUB(MAX(value_at), INTERVAL {data_depth}+1 MONTH)
                    FROM stock_data_daily
                    WHERE ticker_id = {ticker_id}
                )
            ORDER BY value_at ASC;
        """
        data = connector.custom_query(query)
        
        data = indicators.obtain_metrics(data)
        list_of_data = data.to_dict(orient='records')
        
        # Se actualizan los datos
        connector.update_data('stock_data_daily', list_of_data, 'id', ['rsi', 'aroon_up', 'aroon_down', 'macd', 'macd_hist', 'macd_signal', 'obv', 'adx', 'atr'])

        logging.info(f"Datos a computar obtenidos")
        
    tickers = get_tickers()
    get_prices = get_data.expand(ticker=tickers)
    get_indicators = process_data.expand(ticker=tickers)
    
    tickers >> get_prices >> get_indicators

# Create the DAG instance
stock_data_instance = stock_data_dag()
