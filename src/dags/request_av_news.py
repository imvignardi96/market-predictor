import pendulum
import logging
from airflow.decorators import task, dag
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException
from airflow.models import Variable
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from utils.sqlconnector import SQLConnector


# Define your DAG using the @dag decorator
@dag(
    dag_id='alpha_vantage_scrape',
    description='DAG para obtener noticias de Alpha Vantage',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=16,
    max_active_runs=1,
    schedule_interval='*/1 * * * *',  # Every minute
    doc_md=
    """
        #### Documentacion Scraper Alpha Vantage.
        
        El presente DAG se encarga de obtener periodicamente las noticias para los tickers que se encuentran activos en la base de datos.
        Para ello se utiliza un usuario propio de procesamiento completamente diferente al que utiliza airflow para su base de datos propia.
        
        Los pasos que realiza son:
        1) Obtencion de los Tickers activos. Los tickers son los simbolos que representan a una empresa en el mercado de valores.
        2) Obtencion de las noticias. Se obtienen N noticias para cada Ticker. N viene definido por una variable de airflow manipulable en la GUI.
        3) Ingesta en base de datos. La base de datos esta preparada para no insertar noticias repetidas. 
    """
)
def alpha_vantage_dag():
    
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
            Esta tarea obtiene noticias de Yahoo Finance para los tickers.
            
            -**Args**:
                -ticker: Diccionario que contiene informacion del ticker.
                -n_news: Numero de noticias a obtener.
            
            -**Returns**:
                -news_list: Lista de diccionarios que se agrega como LazySequence.
        """
    )
    def get_news(ticker:dict):
        import pandas as pd
        import time
        import uuid
        import requests

        # La informacion basica del ticker
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        # La clave de la API
        api_key = Variable.get('av_secret')
        min_relevance = float(Variable.get('av_min_relevance'))
        number_of_news = int(Variable.get('av_number_news', default_var=1000))
        
        logging.info(f'Variable obtenidas')
        
        # Primero se obtiene la fecha maxima que se tiene en la base de datos
        query = f"SELECT MAX(article_date) FROM news WHERE ticker_id={ticker_id}"
        db_date = connector.custom_query(query)
        
        db_date_value = str(db_date.iloc[0, 0])
        
        if db_date_value in [None, 'None', '', 'nan', 'NaT'] or (isinstance(db_date_value, float) and pd.isna(db_date_value)):
            time_from = Variable.get('av_data_start')
        else:
            time_from=pendulum.parse(db_date_value).strftime('%Y%m%dT%H%M')
            
        logging.info(f'Fecha a utilizar: {time_from}')

        max_date = pendulum.date(2000,1,1)
        overflow = False # Variable para evitar ingestar demasiados datos a la vez. Si es True finaliza la ejecucion
        news_list = []
        while max_date!=pendulum.now().date() and not overflow:
            logging.info(f"Obteniendo noticias de {ticker_code} a partir de la fecha {time_from} con un limite de {number_of_news}")
            url=f'https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers={ticker_code.upper()}&sort=EARLIEST&limit={number_of_news}&time_from={time_from}&apikey={api_key}'
            
            # Realizamos la request a alpha vantage
            r = requests.get(url).json()
            # Esperamos un segundo para no superar el limite de llamadas por minuto (75)
            time.sleep(1)
            
            # Si no se produjo un error, guardamos las noticias de relevancia
            if 'Error Message' not in r:
                for article in r['feed']:
                    for ticker_in_article in article['ticker_sentiment']:
                        if ticker_in_article['ticker'] == ticker_code.upper() and float(ticker_in_article['relevance_score'])>=min_relevance:
                            
                            namespace = uuid.NAMESPACE_DNS  # You can replace with a custom UUID
                            unique_string = f"{ticker_id}-{article['title']}-{article['time_published']}"
                            id = uuid.uuid5(namespace, unique_string)
                                              
                            dict = {
                                'id':str(id),
                                'ticker_id':ticker_id,
                                'article_title':article['title'],
                                'article_date':pendulum.parse(article['time_published'])
                            }
                            news_list.append(dict)
                            
                            if pendulum.parse(article['time_published']).date()>max_date:
                                prev_date = max_date
                                max_date = pendulum.parse(article['time_published']).date()
                                time_from = article['time_published'][:-2]
                                
                            if prev_date==max_date:
                                logging.info('Aun no se recibieron las noticias del dia')
                                break
                            
                                
                if len(news_list)>1000:
                    logging.info('Se ha producido overflow para proteger a la base de datos')
                    overflow=True
            # Si hubo error lanzamos excepcion             
            else:
                logging.error(f"Error API: {r['Error Message']}")
                raise AirflowFailException
                
        return news_list

    @task(
        doc_md=\
        """
            Esta tarea ingesta las noticias en la base de datos.
            Recibe una LazySequence ya que no se puede saber de antemano el numero de tasks dinamicas.
            Se debe tener en cuenta que es mejor obtener pocas noticias continuamente, que muchas pocas veces.
            
            -**Args**:
                -news_data: Lista de diccionarios con los titulares de las noticias.
        """
    )
    def insert_database(news_data):
        # Guardamos las noticias por grupos para evitar sobrecargar la base de datos
        for group_news in news_data:
            connector.insert_data('news', group_news, prefix='IGNORE')

    # Orden de ejecucion
    tickers = get_tickers()
    news = get_news.expand(ticker=tickers)
    insert_database(news)

# Create the DAG instance
alpha_vantage_dag_instance = alpha_vantage_dag()