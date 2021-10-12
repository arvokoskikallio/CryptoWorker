import csv
import pandas as pd
from quantaworker import Column, ImportWorker, ValueType, DataSample, InvocationParam, List, Parameter, ImportWorker

class csvWorker(ImportWorker):

    @property
    def default_name(self) -> str:
        "csvtest"

    @property
    def description(self) -> str:
        return "csv Test worker"

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
            }
            dict_list.append(temp)

        return DataSample(self.columns, pd.DataFrame(data=dict_list))

    def run_import(self, params: List[InvocationParam]) -> pd.DataFrame:
        file = open('btc.csv')
        csvreader = csv.reader(file)

        rows = []
        for row in csvreader:
            rows.append(row)

        print(rows)

        return pd.DataFrame(columns=rows[0], data=rows[1:])