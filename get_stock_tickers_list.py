import requests
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

influxdb_token="-rIXlvsiCqsAnrRAQTksKTxQVgoSV7LmeTSCZBs4oLNh2W2V_bVi6w0t-VAHe2Pqdx39QS8QNC8QthwzbGzxrQ=="
FMP_APIkey="43c5c4379082f96fc11726d27e03d166"

URL = "https://financialmodelingprep.com/api/v3/stock/list?apikey="+FMP_APIkey

org = "DJLinvest"
url = "http://77.38.44.137:8086"

client = influxdb_client.InfluxDBClient(url=url, token=influxdb_token, org=org)
bucket="FMP_static_data"

write_api = client.write_api(write_options=SYNCHRONOUS)
data_points = []

all_tickers = requests.get(URL).json() # get all data from FMP
tags = ["symbol","name","exchange","exchangeShortName","type"]
for i, ticker in enumerate(all_tickers): # create a Point for each ticker and put them into points list
    # print(ticker)
    point = Point("company_info")
    for key, value in ticker.items():
        # print(f"{key} === {value}")
        if key in tags:
            point.tag(key,value)
        elif key == "price":
            if value == None:
                value = 0
            point.field(key,float(value))
    data_points.append(point)
    
# Batch size and delay between batches (in seconds)
batch_size = 1000 # number of companies to be writen to the server at one write
delay = 0.1  # 100ms delay between each batch

# Function to write points in batches
def write_in_batches(data_points, batch_size, delay):
    for i in range(0, len(data_points), batch_size):
        batch = data_points[i:i + batch_size]
        write_api.write(bucket=bucket, org=org, record=batch[:])
        print(f"Written batch {i//batch_size + 1}")
        time.sleep(delay)

# Write data in batches with delay
write_in_batches(data_points, batch_size, delay)

client.close()