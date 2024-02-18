# working for 3 and 5 mints, check for 9 mint also


import websocket
import json
from threading import Thread
import datetime
import asyncio
import pandas as pd
import mysql.connector
import pytz
import time
import ast


# read config file
config_file_path = 'background/security.txt'
with open(config_file_path, 'r') as file:
    content = file.read()

security_config = ast.literal_eval(content)

hostname = security_config["hostname"]
username = security_config["username"]
password = security_config["password"]
database = security_config["database_name"]

connection = mysql.connector.connect(
    host=hostname,
    user=username,
    password=password,
    database=database
)
if connection.is_connected():
    print("Connected to MySQL database")
    cursor = connection.cursor()

# Define the Binance WebSocket URL for Kline (candlestick) data
CO = []
CH = []
CL = []
CC = []
# global np_open_i, np_low_i, np_high_i, np_close_i
cols = ["O", "C", "H", "L"]
five_minute_df = pd.DataFrame(columns=cols)
time_limit = 6
current_iteration = 1
sample_json = {}

# symbols =['ETHBTC', 'LTCBTC', 'BNBBTC', 'NEOBTC', 'QTUMETH', 'EOSETH', 'SNTETH', 'BNTETH', 'BCCBTC', 'GASBTC', 'BNBETH', 'BTCUSDT', 'ETHUSDT', 'HSRBTC', 'OAXETH', 'DNTETH', 'MCOETH', 'ICNETH', 'MCOBTC', 'WTCBTC', 'WTCETH', 'LRCBTC', 'LRCETH', 'QTUMBTC', 'YOYOBTC', 'OMGBTC', 'OMGETH', 'ZRXBTC', 'ZRXETH', 'STRATBTC', 'STRATETH', 'SNGLSBTC', 'SNGLSETH', 'BQXBTC', 'BQXETH', 'KNCBTC', 'KNCETH', 'FUNBTC', 'FUNETH', 'SNMBTC', 'SNMETH', 'NEOETH', 'IOTABTC', 'IOTAETH', 'LINKBTC', 'LINKETH', 'XVGBTC', 'XVGETH', 'SALTBTC', 'SALTETH', 'MDABTC', 'MDAETH', 'MTLBTC', 'MTLETH', 'SUBBTC', 'SUBETH', 'EOSBTC', 'SNTBTC', 'ETCETH', 'ETCBTC', 'MTHBTC', 'MTHETH', 'ENGBTC', 'ENGETH', 'DNTBTC', 'ZECBTC', 'ZECETH', 'BNTBTC', 'ASTBTC', 'ASTETH', 'DASHBTC', 'DASHETH', 'OAXBTC', 'ICNBTC', 'BTGBTC', 'BTGETH', 'EVXBTC', 'EVXETH', 'REQBTC', 'REQETH', 'VIBBTC', 'VIBETH', 'HSRETH', 'TRXBTC', 'TRXETH', 'POWRBTC', 'POWRETH', 'ARKBTC', 'ARKETH', 'YOYOETH', 'XRPBTC', 'XRPETH', 'MODBTC', 'MODETH', 'ENJBTC', 'ENJETH', 'STORJBTC', 'STORJETH', 'BNBUSDT', 'VENBNB', 'YOYOBNB', 'POWRBNB', 'VENBTC', 'VENETH', 'KMDBTC', 'KMDETH', 'NULSBNB', 'RCNBTC', 'RCNETH', 'RCNBNB', 'NULSBTC', 'NULSETH', 'RDNBTC', 'RDNETH', 'RDNBNB', 'XMRBTC', 'XMRETH', 'DLTBNB', 'WTCBNB', 'DLTBTC', 'DLTETH', 'AMBBTC', 'AMBETH', 'AMBBNB', 'BCCETH', 'BCCUSDT', 'BCCBNB', 'BATBTC', 'BATETH', 'BATBNB', 'BCPTBTC', 'BCPTETH', 'BCPTBNB', 'ARNBTC', 'ARNETH', 'GVTBTC', 'GVTETH', 'CDTBTC', 'CDTETH', 'GXSBTC', 'GXSETH', 'NEOUSDT', 'NEOBNB', 'POEBTC', 'POEETH', 'QSPBTC', 'QSPETH', 'QSPBNB', 'BTSBTC', 'BTSETH', 'BTSBNB', 'XZCBTC', 'XZCETH', 'XZCBNB', 'LSKBTC', 'LSKETH', 'LSKBNB', 'TNTBTC', 'TNTETH', 'FUELBTC', 'FUELETH', 'MANABTC', 'MANAETH', 'BCDBTC', 'BCDETH', 'DGDBTC', 'DGDETH', 'IOTABNB', 'ADXBTC', 'ADXETH', 'ADXBNB', 'ADABTC', 'ADAETH', 'PPTBTC', 'PPTETH', 'CMTBTC', 'CMTETH', 'CMTBNB', 'XLMBTC', 'XLMETH', 'XLMBNB', 'CNDBTC', 'CNDETH', 'CNDBNB', 'LENDBTC', 'LENDETH', 'WABIBTC', 'WABIETH', 'WABIBNB', 'LTCETH', 'LTCUSDT', 'LTCBNB', 'TNBBTC', 'TNBETH', 'WAVESBTC', 'WAVESETH', 'WAVESBNB', 'GTOBTC', 'GTOETH', 'GTOBNB', 'ICXBTC', 'ICXETH', 'ICXBNB', 'OSTBTC', 'OSTETH']

# read symbols from database from table symbols
query = "SELECT * FROM symbols where active = 1"
cursor.execute(query)
data = cursor.fetchall()

symbols = [symbol[1] for symbol in data]

# Print the result
print("Extracted Symbols:", symbols)
k_line_array = ["3m", "5m"]
base_url = "wss://fstream.binance.com/stream?streams="
print(symbols.__len__())
for symbol in symbols:
    # print("symbol: ", symbol)
    sample_json[symbol] = {"candle_data": []}
    for k_line in k_line_array:
        # print("kline: ", k_line)
        base_url = base_url + symbol.lower() + "@kline_" + k_line
        if current_iteration < symbols.__len__():
            base_url = base_url + "/"
    current_iteration += 1
print(base_url)
binance_websocket_url = base_url
# binance_websocket_url_3m = "wss://stream.binance.com:9443/stream?streams=btcusdt@kline_3m/ethusdt@kline_3m/maticusdt@kline_3m"


# Define the list of symbols you want to stream

def wsconnect():
    "******************"
    print("Connecting to Binance WebSocket...")
    ws = websocket.WebSocketApp(binance_websocket_url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()


def on_message(ws, message):
    # print("Hi")
    try:
        json_data = json.loads(message)
        # Print or process the candlestick data as needed
        global CO, CH, CL, CC
        is_candle_closed = json_data['data']['k']['x']
        # print(json_data)
        current_time = datetime.datetime.now()
        # print("Current Time: ", current_time)
        if is_candle_closed:    
            # print(json_data)
            k_line = json_data['data']['k']['i']
            symbol = json_data['stream'].split('@')[0]
            symbol = symbol.upper()
            open = json_data['data']['k']['o']
            high = json_data['data']['k']['h']
            low = json_data['data']['k']['l']
            close = json_data['data']['k']['c']
            event_time = json_data['data']['E']
            open_time = json_data['data']['k']['t']
            open_time = int(open_time / 1000.0)
            event_time = int(event_time / 1000.0)
            query = "INSERT INTO tick_data_min(timeframe,s,E,o,h,l,c,ist,utc) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            utc_datetime = datetime.datetime.utcfromtimestamp(open_time)
            print("UTC Time: ", utc_datetime)
            current_time = datetime.datetime.now()
            # print("current_time: ", current_time)
            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(ist_timezone)
            print("ist_datetime: ", ist_datetime)
            ist_minute = ist_datetime.minute
            id = str(current_time) + "_" + symbol + "_" + k_line
            cursor.execute(query, (k_line, symbol, str(event_time), str(open), str(high), str(low),
                                str(close), str(ist_datetime), str(utc_datetime)))
            connection.commit()
            print("Data inserted to minutes table for", k_line, "min for symbol :", symbol)
            if k_line == "3m":
                current_hour = current_time.hour
                check_ist_minute = ist_minute + 3
                if check_ist_minute >= 60:
                    check_ist_minute = check_ist_minute - 60
                if (
                        (((current_hour % 3 == 1) and ((check_ist_minute) % 9 == 0))) or
                        (((current_hour % 3 == 2) and ((check_ist_minute) % 9 == 3))) or
                        (((current_hour % 3 == 0) and ((check_ist_minute) % 9 == 6)))
                ):
                    print("current_hour: ", current_hour)
                    print("Initializing 9m data")
                    k_line = "9m"
                    id = str(current_time) + "_" + symbol + "_" + k_line
                    # print(f"symbol: {symbol}")
                    query = "SELECT * FROM tick_data_min WHERE s = %s AND timeframe = %s ORDER BY ist DESC LIMIT 3"
                    cursor.execute(query, (symbol, "3m"))
                    data = cursor.fetchall()
                    print("Data: ", data)
                    # open = data[0][4]
                    open = data[2][4]
                    print(f"9m open: {open}")
                    high = max(data[0][5], data[1][5], data[2][5])
                    low = min(data[0][6], data[1][6], data[2][6])
                    close = data[0][7]
                    print("Open: ", open)
                    print("High: ", high)
                    print("Low: ", low)
                    print("Close: ", close)
                    # Subtract 6 minutes from ist_datetime
                    ist_datetime_minus_6 = ist_datetime - datetime.timedelta(minutes=6)
                    print("IST Time - 6 minutes: ", ist_datetime_minus_6)
                    # Subtract 6 minutes from utc_datetime
                    utc_datetime_minus_6 = utc_datetime - datetime.timedelta(minutes=6)
                    print("UTC Time - 6 minutes: ", utc_datetime_minus_6)
                    # insert the data to the table
                    query = "INSERT INTO tick_data_min(timeframe,s,E,o,h,l,c,ist,utc) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    cursor.execute(query, (k_line, symbol, str(event_time), str(open), str(high),
                                        str(low), str(close), str(ist_datetime_minus_6), str(utc_datetime_minus_6)))
                    connection.commit()
                    print("Data inserted to current_db table for ", k_line, "min for symbol :", symbol)

            ## UPDATE THE CURRENT TABLE

            # check if the table is empty
            query = "SELECT * FROM tick_data_last WHERE s = %s AND timeframe = %s"
            cursor.execute(query, (symbol, k_line))
            data = cursor.fetchall()
            if data.__len__() == 0:
                query = "INSERT INTO tick_data_last(timeframe,s,E,o,h,l,c) VALUES (%s,%s,%s,%s,%s,%s,%s)"
                id = symbol + "_" + k_line
                cursor.execute(query, (k_line, symbol, str(event_time), str(open), str(high), str(low), str(close)))
                connection.commit()
                print("Data inserted to current_db table for", k_line)
            # update the current table where symbol = symbol and k_line = k_line
            else:
                # query = "UPDATE current_table SET open = %s, high = %s, low = %s, close = %s, e = %s, ist = %s, utc = %s WHERE s = %s AND timeframe = %s"
                query = "UPDATE tick_data_last SET o = %s, h= %s, l = %s, c = %s, E = %s WHERE s = %s AND timeframe = %s"
                cursor.execute(query, (str(open), str(high), str(low), str(close), str(event_time), symbol, k_line))
                connection.commit()
            # print("Data inserted to current_db table for", k_line)

            if (current_time.hour == 0 and current_time.minute == 23 and current_time.second == 13) or \
                    (current_time.hour == 4 and current_time.minute == 23 and current_time.second == 13) or \
                    (current_time.hour == 9 and current_time.minute == 12 and current_time.second == 13) or \
                    (current_time.hour == 12 and current_time.minute == 12 and current_time.second == 13) or \
                    (current_time.hour == 16 and current_time.minute == 42 and current_time.second == 13) or \
                    (current_time.hour == 20 and current_time.minute == 32 and current_time.second == 13):
                print("Market is closed")
                ws.close()
                wsconnect()
    except Exception as e:
        print("Exception: ", e)
        ws.close()
        time.sleep(40)



def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("Connection closed")
    wsconnect()


async def main():
    # Create a WebSocket connection
   while True:
        wsconnect()


if __name__ == "__main__":
    asyncio.run(main())

