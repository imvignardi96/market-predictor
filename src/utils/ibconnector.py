from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.connection import Connection
from ibapi.common import * 
import threading
import time
import logging

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
            self.historical_data = []
            self.next_valid_id_event = threading.Event()
            self.data_ready_event = threading.Event()
            self.__initialized = True
            
    def nextValidId(self, orderId: int):
        """
            Siguiente orden valida. 
            Cuando la conexion se completa o se ejecuta un comando se recibe esta respuesta.
        """
        logging.info(f"Received nextValidId: {orderId}")
        self.next_valid_id_event.set()
            
    def connect_ib(self, host, port, client):
        if not self.isConnected():
            self.connect(host, port, client)
            thread = threading.Thread(target=self.run, daemon=True)
            thread.start()

            # Esperar proximo nextValidId
            logging.info("Esperando nextValidId...")
            if not self.next_valid_id_event.wait(timeout=5):  # 5 segundos
                logging.error("Se produjo Timeout esperando nextValidId!")
                return False  # Connection failed
            
            logging.info("Conectado a Interactive Brokers!")
            return True
         
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
        self.data_ready_event.set()  # Datos listos. Permite proxima ejecucion
        logging.info(f"Estado 3 del flag: {self.data_ready_event.is_set()}")