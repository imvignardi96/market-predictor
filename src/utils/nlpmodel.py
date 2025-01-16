from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline
import numpy as np
from dataclasses import dataclass

@dataclass
class NlpModel:
    """
    Modelo NLP para analisis de sentimientos.
    Computa los logits del modelo para obtener un valor connntinnuo que identifique como 
    de positivo es el sentimiento. ATENCION: 
        Solo valido para output de 3 neuronas (3 logits)
        Solo valido si las neuronas tinen orden positivo->negativo->neutro
        
    Args:
        model_name: Nombre del modelo a utilizar. Definible en la UI de Airflow.

    Returns:
        sentiment_score: Senntimiento en formato numerico continuo
    """
    model_name:str
    
    def __post_init__(self):
        self.tokenizer = BertTokenizer.from_pretrained(self.model_name)
        self.model = BertForSequenceClassification.from_pretrained(self.model_name, num_labels=3)
        

    # Calcula el senntimiento en base a los logits
    def _calculate_sentiment_score(self, logits):
        """
        Obtener score en base a logits.
        
        Los logits proporcionados por el modelo deben ser positivo->negativo->neutro
        """
        # Apply softmax to logits to get probabilities
        exp_logits = np.exp(logits)
        probabilities = exp_logits / np.sum(exp_logits)
        prob_positive, prob_negative, prob_neutral = probabilities

        # Sentimiento como positivo - negativo. Neutral sin peso.
        sentiment_score = prob_positive - prob_negative
        return sentiment_score

    # Obtener los sentimientos y los logits asociados a cada output
    def analyze_sentiment_with_score(self, texts:list[str]) -> list[float]:
        """
        A partir de una lista de textos, obtiene el sentimiento como
        valor numerico.

        Args:
            texts (list[str]): Lista de titulos/textos a analizar

        Returns:
            list[float]: Lista de scores de sentimientos.
        """
        # Tokenize input text and get raw model outputs
        inputs = self.tokenizer(texts, return_tensors="pt", padding=True, truncation=True)
        outputs = self.model(**inputs)

        # Extraer los logits que identifican la probabilidad de pertenecer a una clase.
        logits = outputs.logits.detach().numpy()
        scores = [round(self._calculate_sentiment_score(logit).item(), 3) for logit in logits]
        
        return scores