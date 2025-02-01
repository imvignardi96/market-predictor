from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.connection import Connection
from ibapi.common import * 
import threading

class IBApi(EWrapper, EClient):
    instance = None  # Singleton instance

    def __new__(cls):
        if cls.instance is None:
            cls.instance = super(IBApi, cls).__new__(cls)
            cls.instance.__initialized = False
        return cls.instance

    def __init__(self):
        if not self.__initialized:
            EClient.__init__(self, self)
            self.__initialized = True
            
    def connect_ib(self, host, port):
        if not self.isConnected():
            self.connect(host, port, 1)
            thread = threading.Thread(target=self.run, daemon=True)
            thread.start()
         
    def historicalData(self, reqId: int, bar: BarData):
        data = {
            "value_at": bar.date,
            "opening_price": bar.open,
            "high_price": bar.high,
            "low_price": bar.low,
            "closing_price": bar.close,
            "volume": bar.volume
        }
        self.historical_data.append(data)
        
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        logging.info(f"Datos historicos obtenidos. Req: {reqId}, Start: {start}, End: {end}")
        self.data_ready = True  # Set flag when data retrieval is complete  