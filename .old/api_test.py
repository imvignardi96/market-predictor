from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.connection import Connection
from ibapi.contract import Contract
from ibapi.common import *  # @UnusedWildImport
from datetime import datetime, timedelta
import pandas as pd
import threading
import time

depth = 1
start_date = datetime.strptime('20050527 23:59:00', '%Y%m%d %H:%M:%S')
ticker = 'TSLA'
timestep = 30

class IBApi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self) 

        self.historical_data = []
        self.no_data_tickers = set()
         
    def historicalData(self, reqId: int, bar: BarData):
        data = {
            "Date": bar.date,
            "Open": bar.open,
            "High": bar.high,
            "Low": bar.low,
            "Close": bar.close,
            "Volume": bar.volume
        }
        # print(bar.date)
        print(data)
        self.historical_data.append(data)
        
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("Historical data retrieval completed")
        self.data_ready = True  # Set flag when data retrieval is complete
        
    def error(self, reqId, errorCode, errorString):
        """Handle errors from TWS"""
        if errorCode == 162:  # HMDS query returned no data
            print(f"❌ No data available for reqId {reqId}")
            self.no_data_tickers.add(reqId)

def run_loop():
    app.run()    
    
app = IBApi()
app.connect("127.0.0.1", 7497, 123)  # Make sure TWS or IB Gateway is running on this port

api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

time.sleep(1)  # Sleep interval to allow time for connection to server

contract = Contract()
contract.symbol = ticker
contract.secType = "STK"
contract.exchange = "SMART"
contract.currency = "USD"

app.data_ready = False
execution_date = start_date.strftime('%Y%m%d %H:%M:%S')
req_id = 1
app.reqHistoricalData(req_id, contract, execution_date, "1 W", f"{timestep} mins", "TRADES", 1, 1, False, [])

# Wait until data is ready
while depth > 0:
    if app.data_ready:
        start_date = start_date-timedelta(weeks=1)
        print(start_date)
        print(depth)
        execution_date = start_date.strftime('%Y%m%d %H:%M:%S')
        req_id = req_id+1
        
        app.reqHistoricalData(req_id, contract, execution_date, "1 W", f"{timestep} mins", "TRADES", 1, 1, False, [])
        
        app.data_ready = False
        depth = depth - 1
    elif req_id in app.no_data_tickers:
        print('Finishing this instance')
        app.disconnect()
        break
        
    time.sleep(1)

# Convert historical data to DataFrame
df = pd.DataFrame(app.historical_data)

app.disconnect()

df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d %H:%M:%S %Z')
df.index = df['Date']
df.drop(['Date'], axis =1, inplace=True)
df.sort_index(ascending=False, inplace=True)

df.to_csv(f'{ticker}_{timestep}.csv', sep=';')


