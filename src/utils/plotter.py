import matplotlib.pyplot as plt
import os
import numpy as np
from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error, mean_absolute_error, r2_score, root_mean_squared_error

class LSTMPlotter:
    def __init__(self):
        pass # Incluir a futuro grafica con ejes
    
    def add_plot(self, y_test, y_pred, model_path:str):
        y_test = np.ravel(y_test)
        y_pred = np.ravel(y_pred)
    
        # Computar metricas
        # 1. De error
        mse = mean_squared_error(y_test, y_pred)
        mape = mean_absolute_percentage_error(y_test, y_pred)

        # 2. De precision direccional
        directional_accuracy = np.mean((np.sign(np.diff(y_test)) == np.sign(np.diff(y_pred))).astype(int))

        # 3. De correlacion
        r2 = r2_score(y_test, y_pred)
        pearson_corr = np.corrcoef(y_test, y_pred)[0, 1]


        # Graficar resultados
        plt.figure(figsize=(30, 20))
        plt.plot(y_test, color='blue', label='Actual Closing Price')
        plt.plot(y_pred, color='red', label='Predicted Closing Price')

        # Titulo y labels
        plt.title('Stock Closing Price Prediction', fontsize=20)
        plt.xlabel('Time', fontsize=16)
        plt.ylabel('Stock Closing Price', fontsize=16)
        plt.legend(fontsize=14)

        # Mostrar metricas principales
        metrics_text = f"""MAPE: {mape:.2f}%
        \nMSE: {mse:.2f}
        \nDA: {directional_accuracy:.2f}%
        \nR²: {r2:.4f}
        \nPearson: {pearson_corr:.4f}
        """
        plt.text(2, max(y_test) * 0.995, metrics_text, fontsize=18, color='black', 
                bbox=dict(facecolor='white', alpha=0.6))

        # Show plot
        plt.savefig(model_path)
        
        return mape, directional_accuracy, r2
            
    def show(self):
        # En caso de querer mostrar la figura enn alguna situacion
        plt.tight_layout()
        plt.show()
