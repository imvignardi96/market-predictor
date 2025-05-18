import pendulum
from airflow.sdk import task, dag
from airflow.models.variable import Variable
from airflow.exceptions import AirflowSkipException
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from utils.sqlconnector import SQLConnector

class Deprecated(AirflowSkipException):
    def __init__(self, message):
        super().__init__(message)


# Define your DAG using the @dag decorator
@dag(
    dag_id='yfinance_scrape',
    description='DAG para obtener noticias de Yahoo Finance',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=16,
    max_active_runs=1,
    schedule='*/10 * * * *',  # Every 10 minutes
    doc_md=
    """
        ⚠️ OBSOLETO ⚠️

        #### Documentacion Scraper Yahoo Finance.
        
        El presente DAG se encarga de obtener periodicamente las noticias para los tickers que se encuentran activos en la base de datos.
        Para ello se utiliza un usuario propio de procesamiento completamente diferente al que utiliza airflow para su base de datos propia.
        
        Los pasos que realiza son:
        1) Obtencion de los Tickers activos. Los tickers son los simbolos que representan a una empresa en el mercado de valores.
        2) Obtencion de las noticias. Se obtienen N noticias para cada Ticker. N viene definido por una variable de airflow manipulable en la GUI.
        3) Ingesta en base de datos. La base de datos esta preparada para no insertar noticias repetidas. 
    """
)
def yfinance_dag():
    
    db_user = Variable.get('db_user')
    db_pwd = Variable.get('db_password')
    number_of_news = int(Variable.get('yf_number_news', default_var=5))
                         
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
    def get_news(ticker:dict, n_news:int):
        import yfinance as yf

        # This will fetch news for the given ticker
        ticker_code = ticker['ticker']
        ticker_id = ticker['id']
        
        ticker = yf.Ticker(ticker_code)
        news = ticker.get_news(count=n_news)
        
        news_list = []
        for new in news:
            dict = {
                'id':new['id'],
                'ticker_id':ticker_id,
                'article_title':new['content']['title'],
                'article_date':pendulum.parse(new['content']['pubDate'])
            }
            news_list.append(dict)
        
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
        # Ingest data into the database
        # We have to be careful of the size of the LazySequence, it could cause major performance
        # Impact, hence it is better many executions but with low ammount of news
        raise Deprecated('No se realizan cargas en la base de datos mediante este DAG')
        for group_news in news_data:
            # Each position contains a list of dicts. Insert in batces of tickers
            connector.insert_data('news', group_news, prefix='IGNORE')

    # Workflow logic: Get tickers, fetch news, then ingest into DB
    tickers = get_tickers()
    news = get_news.partial(n_news=number_of_news).expand(ticker=tickers)
    insert_database(news)

# Create the DAG instance
yfinance_dag_instance = yfinance_dag()