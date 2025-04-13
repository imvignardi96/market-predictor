import numpy as np
import pandas as pd
from dataclasses import dataclass

@dataclass
class technicalIndicators:
    """
    Inicializa la clase con los parametros a utilizar en la computacion
    """
    # RSI PARAMS
    rsi_period:int = 7
    
    # AROON PARAMS
    aroon_period:int = 14
    
    #MACD PARAMS
    macd_fast:int = 12
    macd_slow:int = 26
    macd_signal:int = 9
    
    # ADX PARAMS
    adx_period: int = 14

    # ATR PARAMS
    atr_period: int = 14
    
    def obtain_metrics(self, df:pd.DataFrame) -> pd.DataFrame:
        """
        Obtiene ciertos indicadores tecnicos muy utilizados en la prediccion del mercado de valores.
        En la documentacion proporcionada se puede encontrar mas sobre estos indicadores.
        Args:
            df (pd.DataFrame): Dataframe en el que integrar los indicadores tecnicos

        Returns:
            pd.DataFrame: Retorna el dataframe con las nuevas columnas.
        """
        self.df = df.copy()
        
        self._compute_rsi()
        self._compute_aroon()
        self._compute_obv()
        self._compute_macd()
        self._compute_adx()
        self._compute_atr()
        
        return self.df
    
    def _compute_obv(self):
        """
        Incluye on-balance volume sobre un dataframe
        """
        self.df["obv"] = np.where(self.df["closing_price"] > self.df["closing_price"].shift(1), self.df["volume"], 
             np.where(self.df["closing_price"] < self.df["closing_price"].shift(1), -self.df["volume"], 0))
        
        self.df["obv"] = self.df["obv"].cumsum()
    
    def _compute_rsi(self):
        """
        Incluye el RSI sobre un dataframe
        """
        delta = self.df['closing_price'].diff()
        delta = delta.dropna()

        gain, loss = delta.clip(lower=0), delta.clip(upper=0, lower=None)

        # Use Exponential Moving Average for smoother RSI
        ema_up = gain.ewm(alpha=1/self.rsi_period, min_periods=self.rsi_period).mean()
        ema_down = loss.abs().ewm(alpha=1/self.rsi_period, min_periods=self.rsi_period).mean()

        rs = ema_up / ema_down
        self.df['rsi'] = 100 - (100 / (1 + rs))

    def _compute_aroon(self):
        """
        Coomputa los inndicadores AroonUp y AroonDown

        - Aroon Up: Medida de hace cuanto se produjo el mayor "high"
        - Aroon Down: Medida de hace cuanto se produjo el menor "low"
        """
        # Computar Aroon Up
        self.df['aroon_up'] = (self.df['high_price']
                                .rolling(window=self.aroon_period+1, min_periods=self.aroon_period)
                                .apply(lambda x: (x.argmax() / self.aroon_period), raw=True)*100
        )
        
        # Computar Aroon Up
        self.df['aroon_down'] = (self.df['low_price']
                                    .rolling(window=self.aroon_period+1, min_periods=self.aroon_period)
                                    .apply(lambda x: (x.argmin() / self.aroon_period), raw=True)*100
        )
    
    def _compute_macd(self):
        """
        Incluye MACD, Senal y Histograma MACD sobre un dataframe.        
        """
        # Calculate the fast and slow EMAs
        self.df['ema_fast'] = self.df['closing_price'].ewm(span=self.macd_fast, adjust=False).mean()
        self.df['ema_slow'] = self.df['closing_price'].ewm(span=self.macd_slow, adjust=False).mean()
        
        # Calculate the MACD
        self.df['macd'] = self.df['ema_fast'] - self.df['ema_slow']
        
        # Calculate the Signal line
        self.df['macd_signal'] = self.df['macd'].ewm(span=self.macd_signal, adjust=False).mean()
        
        # Calculate the MACD Histogram
        self.df['macd_hist'] = self.df['macd'] - self.df['macd_signal']
        
        # Drop the intermediate EMA columns
        self.df.drop(columns=['ema_fast', 'ema_slow'], axis=1, errors='ignore', inplace=True)
        
    def _compute_adx(self):
        high = self.df['high_price']
        low = self.df['low_price']
        close = self.df['closing_price']

        plus_dm = high.diff()
        minus_dm = low.diff()

        plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
        minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = np.maximum.reduce([tr1, tr2, tr3])
        atr = pd.Series(tr).rolling(window=self.adx_period).mean()

        plus_di = 100 * (pd.Series(plus_dm).rolling(window=self.adx_period).sum() / atr)
        minus_di = 100 * (pd.Series(minus_dm).rolling(window=self.adx_period).sum() / atr)

        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx = dx.rolling(window=self.adx_period).mean()

        self.df['adx'] = adx

    def _compute_adx(self):
        high = self.df['high_price'].astype(float)
        low = self.df['low_price'].astype(float)
        close = self.df['closing_price'].astype(float)

        plus_dm = high.diff()
        minus_dm = low.diff()

        plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
        minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)

        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        
        # Ensure values are float to avoid Decimal issues
        tr = pd.concat([tr1, tr2, tr3], axis=1).astype(float).max(axis=1)

        atr = tr.rolling(window=self.atr_period).mean()
        plus_di = 100 * pd.Series(plus_dm).rolling(window=self.atr_period).mean() / atr
        minus_di = 100 * pd.Series(minus_dm).rolling(window=self.atr_period).mean() / atr
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100

        adx = dx.rolling(window=self.atr_period).mean()
        self.df['adx'] = adx