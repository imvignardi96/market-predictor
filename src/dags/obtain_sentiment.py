from utils.sqlconnector import SQLConnector
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException
from airflow.models import Variable
import torch
import pendulum
import logging
from airflow.decorators import task, dag

# Define your DAG using the @dag decorator
@dag(
    dag_id='yfinance_scrape',
    description='DAG para obtener noticias de Yahoo Finance',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=16,
    max_active_runs=1,
    schedule_interval='*/10 * * * *',  # Every 10 minutes
    doc_md=
    """
        #### Documentacion Sentiment Analyzer.
        
        El DAG se encarga de obtener periodicamente el sentimiento de las noticias financieras que se encuentran en la tabla market_predictor.news.
        Para ello se utiliza un modelo preentrenado cuyo objetivo es este mismo, finbert.
        
        Los pasos que realiza son:
        1) Obtencion de las noticias cuyo sentimiento no fue calculado (NULL).
        2) Calculo del sentimiento mediante torch.
        3) Ingesta en base de datos. Actualiza las noticias existentes con el sentimiento. 
    """
)
def sentiment_dag():
    
    db_user = Variable.get('db_user')
    db_pwd = Variable.get('db_password')
                         
    connector = SQLConnector(username=db_user, password=db_pwd)
    
    @task(
        doc_md=
        """
            Esta tarea obtiene las noticias sobre las que calcular el sentimiento.
            
            -**Returns**:
                -no_sentiment_news: Lista de tuplas con las noticias donde no se calculo sentimiento.
        """
    )
    def get_news():
        # Fetch the tickers
        required_columns = ['id', 'article_title']
        news_data = connector.read_data('tickers', {'sentiment':None})
        no_sentiment_news = news_data[required_columns].to_dict(orient='records')

        return no_sentiment_news
    
    @task(
        doc_md=
        """
            Esta tarea obtiene el sentimiento de la noticias.
            
            -**Args**:
                -no_sentiment_news: Task get_news que devuelve la lista de listas.
                Se obtiene de esta manera ya que facilita la posterior separacion de los elementos y calculo de sentimiento.
            
            -**Returns**:
                -sentiment_news: Lista de diccionarios con con las noticias y su sentimiento.
        """
    )
    def process_news(no_sentiment_news):
        from transformers import BertTokenizer, BertForSequenceClassification
        from transformers import pipeline
        from tqdm import tqdm
        
        # Inicializamos variables
        batch_size = Variable.get('sentiment_batch_size')
        
        # Cargar FinBERT
        model_name = "ProsusAI/finbert"  # FinBERT for financial sentiment
        tokenizer = BertTokenizer.from_pretrained(model_name)
        model = BertForSequenceClassification.from_pretrained(model_name)
        
        logging.info('Modelo creado')
        
        # Creacion pipeline analisis sentimient
        sentiment_analyzer = pipeline("analisis-sentimiento", model=model, tokenizer=tokenizer)
        
        logging.info('Pipeline establecido')
        
        # Creamos batches 
        batched_articles = [no_sentiment_news[i:i + no_sentiment_news] for i in range(0, len(no_sentiment_news), batch_size)]
        
        # Procesar batches
        all_sentiment_news = []
        for batch in tqdm(batched_articles, desc="Procesando Sentimiento"):
            titles = [article["article_title"] for article in batch] 
            sentiments = sentiment_analyzer(titles)
            
            # Incluir resultados
            for article, sentiment in zip(batch, sentiments):
                article["label"] = sentiment["label"]
                article["score"] = sentiment["score"]

            all_sentiment_news.append(article)
            
        logging.info('Análisis de sentimientos completado')
        
        return all_sentiment_news
        
    @task(
        doc_md=
        """
            Esta tarea actualiza el sentimiento de la noticias en la base de datos.
            
            -**Args**:
                -senntimnt_news: Task process_news que devuelve las noticias y sus setimientos.
        """
    )
    def update_news(sentiment_news):
        print(sentiment_news)
        
    get = get_news()
    process = process_news(get)
    update = update_news(process)
        
dag_instance = sentiment_dag()