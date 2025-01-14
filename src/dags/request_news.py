import pendulum
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException
from airflow.models import Variable
import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from utils.sqlconnector import SQLConnector


# Define your DAG using the @dag decorator
@dag(
    dag_id='yfinance_scrape',
    description='DAG para obtener noticias de Yahoo Finance',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=16,
    max_active_runs=1,
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    doc_md='Documentación DAG aquí'
)
def yfinance_dag():
    
    db_user = Variable.get('db_user')
    db_pwd = Variable.get('db_password')
    number_of_news = int(Variable.get('number_news', default_var=5))
                         
    connector = SQLConnector(username=db_user, password=db_pwd)

    @task(
        doc_md='Esta tarea obtiene los tickers necesarios para el scraping.'
    )
    def get_tickers():
        # Fetch the tickers
        ticker_data = connector.read_data('tickers', {'active':1})
        active_tickers = ticker_data.to_dict(orient='records')

        return active_tickers

    @task(
        doc_md='Esta tarea obtiene noticias de Yahoo Finance para los tickers.'
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

    @task(
        doc_md='Esta tarea ingesta las noticias en la base de datos.'
    )
    def insert_database(news_data):
        # Ingest data into the database
        # You can replace this with actual database insertion code
        print(news_data)

    # Workflow logic: Get tickers, fetch news, then ingest into DB
    tickers = get_tickers()
    news = get_news.partial(n_news=number_of_news).expand(ticker=tickers)
    insert_database(news)

# Create the DAG instance
yfinance_dag_instance = yfinance_dag()

# if __name__=='__main__':
#     yfinance_dag_instance.test()