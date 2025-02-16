from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.connection import Connection
from ibapi.common import * 
import threading
import re
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
            self.error_tickers = None
            self.error_code = None
            self.next_valid_id_event = threading.Event()
            self.data_ready_event = threading.Event()
            self.data_ready_event.set()
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
        
    def error(self, reqId, errorCode, errorString):
        tws_warnnings = [2100, 2101, 2102, 2103, 2104, 2105, 2158, 2106, 2107, 2108, 2109, 2110, 2137, 2168, 2169]
        """Funcion de manejo de errores IB API"""
        if errorCode == 162:  # HMDS query returned no data
            logging.warning(f"Datos no disponibles en IB: {reqId}")
            self.error_code = errorCode
            self.error_tickers = reqId
        elif errorCode == 502:
            logging.error("Ya existe una conexion con IB")
            self.error_code = errorCode
            self.error_tickers = reqId
        elif errorCode == 503:
            logging.error("Se necesita actualizar la plataforma")
            self.error_code = errorCode
            self.error_tickers = reqId
        elif errorCode == 504:
            logging.error("No conectado")
            self.error_code = errorCode
            self.error_tickers = reqId
        elif not str(errorCode).startswith('21'):
            logging.error(f"Error de TWS: {errorString}")
            self.error_code = errorCode
            self.error_tickers = reqId
        else:
            logging.info(f"Mensaje del sistema: {errorString}")