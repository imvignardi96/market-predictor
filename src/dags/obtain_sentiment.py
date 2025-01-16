from utils.sqlconnector import SQLConnector
from airflow.decorators import task, dag
from airflow.exceptions import AirflowException, AirflowSkipException, AirflowFailException
from airflow.models import Variable
import pendulum
import logging

# Dag
@dag(
    dag_id='sentiment_processor',
    description='DAG para obtener los sentimientos de noticias',
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    schedule_interval='*/20 * * * *',  # Cada 20 mintos
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
        # Obtenemos las noticias
        required_columns = ['id', 'article_title']
        news_data = connector.read_data('news', {'sentiment':None})
        no_sentiment_news = news_data[required_columns].to_dict(orient='records')

        return no_sentiment_news
    
    @task(
        doc_md=
        """
            Esta tarea obtiene el sentimiento de la noticias. 
            Utiliza la clase NlpModel, la cual es propia y proporciona una funcion para retornar los scores.
            
            -**Args**:
                -no_sentiment_news: Task get_news que devuelve la lista de listas.
                Se obtiene de esta manera ya que facilita la posterior separacion de los elementos y calculo de sentimiento.
            
            -**Returns**:
                -sentiment_news: Lista de diccionarios con con las noticias y su sentimiento.
        """
    )
    def process_news(no_sentiment_news):
        from tqdm import tqdm
        from utils.nlpmodel import NlpModel
        
        # Inicializamos variables
        batch_size = int(Variable.get('sentiment_batch_size'))
        selected_model = Variable.get('model')
        
        model = NlpModel(selected_model)
        
        logging.info('Modelo creado')
        
        # Creamos batches 
        batched_articles = [no_sentiment_news[i:i + batch_size] for i in range(0, len(no_sentiment_news), batch_size)]
        
        # Procesar batches
        all_sentiment_news = []
        for batch in tqdm(batched_articles, desc="Procesando Sentimiento"):
            titles = [article["article_title"] for article in batch] 
            sentiment_scores = model.analyze_sentiment_with_score(titles)
            
            # Incluir resultados
            for article, sentiment in zip(batch, sentiment_scores):
                article["sentiment"] = sentiment

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
    update_news(process)
        
dag_instance = sentiment_dag()