import pandas as pd
import requests
import json
from datetime import datetime
from quantaworker import Column, ImportWorker, ValueType, DataSample, InvocationParam, List, Parameter, ImportWorker

class cryptoWorker(ImportWorker):

    @property
    def default_name(self) -> str:
        "apitest"

    @property
    def description(self) -> str:
        return "api test 9"

    @property
    def worker_type(self):
        return "Import"

    @property
    def parameters(self) -> List[Parameter]:
        pass

    @property
    def columns(self) -> List[Column]:
        column_list = []
        column_list.append(Column.output("time", "time", ValueType.timestamp_column(), index=0))
        column_list.append(Column.output("open", "open", ValueType.float_column(), index=1))
        column_list.append(Column.output("high", "high", ValueType.float_column(), index=2))
        column_list.append(Column.output("low", "low", ValueType.float_column(), index=3))
        column_list.append(Column.output("close", "close", ValueType.float_column(), index=4))
        column_list.append(Column.output("tradingvolumebtc", "tradingvolumebtc", ValueType.float_column(), index=5))
        column_list.append(Column.output("tradingvolumeusd", "tradingvolumeusd", ValueType.float_column(), index=6))

        return column_list

    def run_sample(self, params: List[InvocationParam]) -> DataSample:

        dict_list = []

        for i in range (1,5):
            temp = {
                "time":"2020-10-05 08:51:00",
                "open":i,
                "high":i,
                "low":i,
                "close":i,
                "tradingvolumebtc":i,
                "tradingvolumeusd":i
            }
            dict_list.append(temp)

        return DataSample(self.columns, pd.DataFrame(data=dict_list))

    def run_import(self, params: List[InvocationParam]) -> pd.DataFrame:
        requestPair = "btcusd"  # btcusd, btceur, ethusd, adausd, xrpusd, etc
        timeScale = "86400"     # set the timescale, see table below

        # Different timescales in seconds
        # 60        1  minute
        # 180       3  minutes
        # 300       5  minutes
        # 900       15 minutes
        # 1800      30 minutes
        # 3600      1  hour
        # 7200      2  hours
        # 14400     4  hours
        # 21600     6  hours
        # 43200     12 hours
        # 86400     1  day
        # 259200    3  days
        # 604800    1  week

        requestUrl = "https://api.cryptowat.ch/markets/bitfinex/" + requestPair + "/ohlc"

        url = requests.get(requestUrl)
        text = url.text

        # takes the data in, and removes the "result" part of the data, i.e. the first column of the array
        data = json.loads(text)["result"]


        # Print all of the info

        columns = ["time", "open", "high", "low", "close", "tradingvolumebtc", "tradingvolumeusd"]
        allData = []

        i = 0
        j = 0
        while i < len(data[timeScale]):
            row=[]
            while j < len(data[timeScale][i]):
                if j == 0:
                    row.append(datetime.utcfromtimestamp(data[timeScale][i][j]).strftime('%Y-%m-%dT%H:%M:%SZ'))
                else:    
                    row.append(str(data[timeScale][i][j]))
                j = j + 1
            allData.append(row)
            j = 0
            i = i + 1

        print(allData)

        return pd.DataFrame(columns=columns, data=allData)