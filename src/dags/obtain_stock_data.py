from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.connection import Connection
from ibapi.common import *  # @UnusedWildImport

import pendulum
from airflow.decorators import task, dag
from airflow.models import Variable

from utils.sqlconnector import SQLConnector

import pandas as pd
import threading
import time

import uuid
import logging

class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self) 

        self.historical_data = []
         
    def historicalData(self, reqId: int, bar: BarData):
        data = {
            "Date": bar.date,
            "Open": bar.open,
            "High": bar.high,
            "Low": bar.low,
            "Close": bar.close,
            "Volume": bar.volume
        }
        # print(bar.date)
        self.historical_data.append(data)
        
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("Historical data retrieval completed")
        self.data_ready = True  # Set flag when data retrieval is complete   
    
@dag(
    dag_id='stock_data_extraction',
    description='DAG para obtener noticias de Yahoo Finance',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=1,
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
    def run_loop():
        app.run() 
    
    ib_host = Variable.get('ib_host')
    ib_port = int(Variable.get('ib_port'))
    
    app = IBApi()
    app.connect(ib_host, ib_port, 1)  # IB TWS debe ejecutarse. 1 es el clientId.
    
    # timestep = 30

    # Debido a que es una API asincrona, mantiene la conexion en ejecucion
    api_thread = threading.Thread(target=run_loop, daemon=True)
    api_thread.start()

    time.sleep(1)  # Intervalo para permitir conexion
    
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
    def get_data(ticker:dict):
        from ibapi.contract import Contract
        
        # Obtener los valores de las columnas
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        contract = Contract()
        contract.symbol = ticker_code
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        
        # Primero debemos obtener la fecha minima que se enncuentra en la bbdd para el ticker
        query = f"SELECT MIN(value_at) FROM stock_daily_data WHERE ticker_id={ticker_id}"
        min_date = connector.custom_query(query)
        
        logging.info(f"Fecha minima de {ticker_code}: {min_date}")
        
        # start_date = pendulum.from_format(Variable.get('data_start'), 'DD-MM-YYYY', tz='UTC')
        # end_date = pendulum.now().strftime('%Y%m%d %H:%M:%S')

        # # queryTime = (datetime.today() - timedelta(days=0)).strftime("%Y%m%d %H:%M:%S")

        # app.data_ready = False
        # execution_date = start_date.strftime('%Y%m%d %H:%M:%S')
        # req_id = uuid.uuid4()
        # app.reqHistoricalData(req_id, contract, execution_date, "1 W", f"{timestep} mins", "TRADES", 1, 1, False, [])

        # # Wait until data is ready
        # while depth > 0:
        #     if app.data_ready:
        #         start_date = start_date-timedelta(weeks=1)
        #         print(start_date)
        #         print(depth)
        #         execution_date = start_date.strftime('%Y%m%d %H:%M:%S')
        #         req_id = req_id+1
                
        #         app.reqHistoricalData(req_id, contract, execution_date, "1 W", f"{timestep} mins", "TRADES", 1, 1, False, [])
                
        #         app.data_ready = False
        #         depth = depth - 1
        # time.sleep(1)

        # # Convert historical data to DataFrame
        # df = pd.DataFrame(app.historical_data)

        # app.disconnect()

        # df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d %H:%M:%S %Z')
        # df.index = df['Date']
        # df.drop(['Date'], axis =1, inplace=True)
        # df.sort_index(ascending=False, inplace=True)

        # df.to_csv(f'{ticker}_{timestep}.csv', sep=';')
        
    tickers = get_tickers()
    get_data.expand(ticker=tickers)

# Create the DAG instance
stock_data_instance = stock_data_dag()

if __name__=='__main__':
    stock_data_instance.test()
