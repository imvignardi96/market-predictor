import numpy as np
import pandas as pd
from dataclasses import dataclass

@dataclass
class technicalIndicators:
    # RSI PARAMS
    rsi_period:int = 7
    
    # AROON PARAMS
    aroon_period:int = 14
    
    #MACD PARAMS
    macd_fast:int = 12
    macd_slow:int = 26
    macd_signal:int = 9
    
    def obtain_metrics(self, df:pd.DataFrame) -> pd.DataFrame:
        self.df = df.copy()
        
        self._compute_rsi()
        self._compute_aroon()
        self._compute_obv()
        self._compute_macd()
        
        return self.df
    
    def _compute_obv(self):
        self.df["obv"] = np.where(self.df["closing_price"] > self.df["closing_price"].shift(1), self.df["volume"], 
             np.where(self.df["closing_price"] < self.df["closing_price"].shift(1), -self.df["volume"], 0))
        
        self.df["obv"] = self.df["obv"].cumsum()
    
    def _compute_rsi(self):
        delta = self.df['closing_price'].diff()
        delta = delta.dropna()

        gain, loss = delta.clip(lower=0), delta.clip(upper=0, lower=None)

        # Use Exponential Moving Average for smoother RSI
        ema_up = gain.ewm(span=1/self.rsi_period, min_periods=self.rsi_period).mean()
        ema_down = loss.ewm(span=1/self.rsi_period, min_periods=self.rsi_period).mean()

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
                               .rolling(window=self.aroon_period, closed='both')
                               .apply(lambda x: ((self.aroon_period - 1) - x.argmax()) / (self.aroon_period) * 100, raw=True)
        )
        
        # Computar Aroon Up
        self.df['aroon_down'] = (self.df['low_price']
                                 .rolling(window=self.aroon_period, closed='both')
                                 .apply(lambda x: ((self.aroon_period) - x.argmin()) / (self.aroon_period) * 100, raw=True)
        )
    
    def _compute_macd(self):
        """
        Adds MACD, Signal line, and MACD Histogram to the DataFrame.
        
        Parameters:
        df (pd.DataFrame): DataFrame containing the 'Close' price column.
        fast_period (int): Period for the fast EMA (default is 12).
        slow_period (int): Period for the slow EMA (default is 26).
        signal_period (int): Period for the signal line EMA (default is 9).
        
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