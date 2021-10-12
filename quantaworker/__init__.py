"""Quanta Worker client."""

from abc import ABC, abstractmethod
import json
import os
import signal
import time
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import pandas as pd
import requests

EXIT = False


TYPE_BOOL = 'java.lang.Boolean'
TYPE_INT = 'java.lang.Long'
TYPE_FLOAT = 'java.lang.Double'
TYPE_STR = 'java.lang.String'
TYPE_TIMESTAMP = 'java.time.Instant'


def _signal_handler(signum, frame):
    """Exit signal handler."""
    global EXIT
    EXIT = True
    _debug("Closing...")


def _debug(message):
    """Baked in debug printing."""
    print("%s - %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), message))


def _auto_cast(value: str, value_type: str) -> Any:
    if value_type == 'java.lang.Boolean':
        return bool(value)

    if value_type == 'java.lang.Double':
        return float(value)

    if value_type == 'java.lang.Long':
        return int(value)

    return value


def _auto_str(value: Any, value_type: str) -> str:
    if value_type == 'java.lang.String':
        return str(value).lower()

    return str(value)


class Parameter:
    """Parameters"""
    def __init__(self, dict: Dict[str, Any]):
        self._name = dict['name']
        self._description = dict['description']
        self._param_type = dict['type']
        self._default = _auto_cast(dict['defaultValue'], self._param_type)
        self._nullable = dict['nullable']

    @staticmethod
    def bool_param(
        name: str,
        description: str,
        default=None,
        nullable=None
    ) -> 'Parameter':
        return Parameter({
            'name': name,
            'description': description,
            'type': TYPE_BOOL,
            'defaultValue': default,
            'nullable': nullable
        })

    @staticmethod
    def int_param(
        name: str,
        description: str,
        default=None,
        nullable=None
    ) -> 'Parameter':
        return Parameter({
            'name': name,
            'description': description,
            'type': TYPE_INT,
            'defaultValue': default,
            'nullable': nullable
        })

    @staticmethod
    def float_param(
        name: str,
        description: str,
        default=None,
        nullable=None
    ) -> 'Parameter':
        return Parameter({
            'name': name,
            'description': description,
            'type': TYPE_FLOAT,
            'defaultValue': default,
            'nullable': nullable
        })

    @staticmethod
    def str_param(
        name: str,
        description: str,
        default=None,
        nullable=None
    ) -> 'Parameter':
        return Parameter({
            'name': name,
            'description': description,
            'type': TYPE_STR,
            'defaultValue': default,
            'nullable': nullable
        })

    @staticmethod
    def timestamp_param(
        name: str,
        description: str,
        default=None,
        nullable=None
    ) -> 'Parameter':
        return Parameter({
            'name': name,
            'description': description,
            'type': TYPE_TIMESTAMP,
            'defaultValue': default,
            'nullable': nullable
        })

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def param_type(self) -> str:
        return self._param_type

    @property
    def default(self) -> Any:
        return self._default

    @property
    def nullable(self) -> bool:
        return self._nullable

    def dict(self) -> Dict[str, Any]:
        return {
            'id': 0,
            'name': self.name,
            'description': self.description,
            'type': self.param_type,
            'defaultValue': _auto_str(self.default, self._param_type),
            'nullable': self.nullable
        }


class ValueType:
    """Column type representation."""

    def __init__(self, dict: Dict[str, Any]):
        self._class_name = dict['className']
        self._format = dict['format']
        self._nullable = dict['nullable']

    @staticmethod
    def bool_column(*, nullable=False, format=None) -> 'ValueType':
        return ValueType({
            'className': TYPE_BOOL,
            'format': format,
            'nullable': nullable
        })

    @staticmethod
    def int_column(*, nullable=False, format=None) -> 'ValueType':
        return ValueType({
            'className': TYPE_INT,
            'format': format,
            'nullable': nullable
        })

    @staticmethod
    def float_column(*, nullable=False, format=None) -> 'ValueType':
        return ValueType({
            'className': TYPE_FLOAT,
            'format': format,
            'nullable': nullable
        })

    @staticmethod
    def str_column(*, nullable=False, format=None) -> 'ValueType':
        return ValueType({
            'className': TYPE_STR,
            'format': format,
            'nullable': nullable
        })

    @staticmethod
    def timestamp_column(*, nullable=False, format="yyyy-MM-dd hh:mm:ss") -> 'ValueType':
        return ValueType({
            'className': TYPE_TIMESTAMP,
            'format': format,
            'nullable': nullable
        })

    @property
    def class_name(self) -> str:
        return self._class_name

    @property
    def format(self) -> str:
        return self._format

    @property
    def nullable(self) -> bool:
        return self._nullable

    def dict(self) -> Dict[str, Any]:
        return {
            'className': self.class_name,
            'format': self.format,
            'nullable': self.nullable
        }


class Column:
    """Column representation."""
    def __init__(self, dict: Dict[str, Any]):
        self._name = dict['name']
        self._description = dict['description']
        self._index = dict['index']
        self._value_type = ValueType(dict['valueType'])
        self._column_type = dict['columnType']

    @staticmethod
    def input(name: str, description: str, value_type: ValueType) -> 'Column':
        return Column({
            'name': name,
            'description': description,
            'index': -1,
            'columnType': 'input',
            'valueType': value_type.dict(),
        })

    @staticmethod
    def output(name: str, description: str, value_type: ValueType, index: int) -> 'Column':
        return Column({
            'name': name,
            'description': description,
            'index': index,
            'columnType': 'output',
            'valueType': value_type.dict(),
        })

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def index(self) -> int:
        return self._index

    @property
    def value_type(self) -> ValueType:
        return self._value_type

    @property
    def column_type(self) -> str:
        return self._column_type

    def dict(self, index_fallback: Optional[int] = None) -> Dict[str, Any]:
        """Convert to Dict."""
        effective_index = self.index
        if effective_index == -1:
            if index_fallback is None:
                raise ValueError(f"Column {self.name} has invalid index ({self.index})")
            effective_index = index_fallback

        return {
            'id': 0,
            'name': self.name,
            'description': self.description,
            'index': effective_index,
            'valueType': self.value_type.dict(),
            'columnType': self.column_type
        }


class InvocationParam:
    """Invocation Parameter representation."""
    def __init__(self, dict: Dict[str, Any]):
        self._name = dict['name']
        self._value = dict['value']

    @property
    def name(self) -> str:
        return self._name

    @property
    def value(self) -> str:
        return self._value

    def parse_value(self, type_str: str) -> Any:
        return _auto_cast(self.value, type_str)


class Invocation:
    """Invocation spec representation."""
    def __init__(self, dict: Dict[str, Any]):
        self._invocation_id = dict['invocationId']
        self._task_type = dict['task']['taskType']
        self._parameters = [InvocationParam(p) for p in dict['parameters']]

    @property
    def invocation_id(self) -> int:
        return self._invocation_id

    @property
    def task_type(self) -> str:
        return self._task_type

    @property
    def parameters(self) -> List[InvocationParam]:
        return self._parameters


class Anomaly:
    """Anomaly representation."""
    def __init__(self, dict: Dict[str, Any]):
        self._start = dict['start']
        self._end = dict['end']
        self._classification = dict['classification']
        self._sample = dict['sample']
        self._probability = dict['probability']
        self._metadata = dict['metadata']

    @property
    def start(self) -> str:
        return self._start

    @property
    def end(self) -> str:
        return self._start

    @property
    def classification(self) -> str:
        return self._classification

    @property
    def sample(self) -> Dict[str, Any]:
        return self._sample

    @property
    def probability(self) -> float:
        return self._probability

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._metadata

    def dict(self) -> Dict[str, Any]:
        """Convert to Dict."""
        return {
            'start': self._start,
            'end': self._end,
            'classification': self._classification,
            'sample': self._sample,
            'probability': self._probability,
            'metadata': self._metadata
        }


class DataSample:
    """Sample of import data."""
    def __init__(self, columns: List[Column], data: pd.DataFrame):
        self._columns = columns
        self._data = data

    @property
    def columns(self) -> List[Column]:
        """List of columns in this sample."""
        return self._columns

    @property
    def data(self) -> pd.DataFrame:
        """Sampled data."""
        return self._data



class QuantaWorker(ABC):
    """Base Quanta Worker."""

    def __init__(self):
        """Initialize the worker."""
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
        self._time_format = os.getenv('TIME_FORMAT')
        self._host = os.getenv('QUANTA_HOST')
        self._port = os.getenv('QUANTA_PORT')
        self._token = os.getenv('QUANTA_WORKER_TOKEN')
        self._name = os.getenv('QUANTA_WORKER_NAME')

        self._url = f"{self._host}:{self._port}/api/worker/v1"

        self._headers = {
            'Content-Type': 'application/json',
            'Authorization': self._token,
        }

    @property
    def name(self) -> str:
        """Return name of this worker."""
        if self._name is None:
            return self.default_name

        return self._name

    @property
    @abstractmethod
    def default_name(self) -> str:
        """Return default name of this worker."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Return description of this worker."""
        pass

    @property
    @abstractmethod
    def worker_type(self):
        """Return type of this worker."""
        pass

    @property
    @abstractmethod
    def parameters(self) -> List[Parameter]:
        """Return parameter spec for this worker."""
        pass

    @property
    @abstractmethod
    def columns(self) -> List[Column]:
        """Return input/output columns for this worker."""
        pass

    @abstractmethod
    def invoke(self, spec: Invocation):
        """Perform work based on invocation spec."""
        pass

    def _auto_gen_indices(self) -> List[Dict[str, Any]]:
        input_columns = [c for c in self.columns if c.column_type == 'input']
        output_columns = [c for c in self.columns if c.column_type == 'output']

        return [c.dict(index) for index, c in enumerate(input_columns)] + \
            [c.dict(index) for index, c in enumerate(output_columns)]

    def run(self):
        """Start polling for tasks."""
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

        self._register()

        while not EXIT:
            time.sleep(5)
            invocation = self._next_invocation()
            if invocation is None:
                continue
            status = self._update_invoke_status(2, invocation)
            _debug(f"Update response code: {str(status)}")
            self.invoke(invocation)
            status = self._update_invoke_status(1, invocation)
            _debug(f"Update response code: {str(status)}")

    def _register(self):
        """Registers worker to Quanta server."""
        registration = {
            'id': 0,
            'definition': {
                'id': -1,
                'type': self.worker_type,
                'name': self.name,
                'description': self.description,
                'columns': [c for c in self._auto_gen_indices()]
            },
            'token': self._token
        }
        try:
            response = requests.post(
                f"{self._url}/register",
                headers=self._headers,
                data=json.dumps(registration)
            )
            print(registration)
        except TimeoutError:
            _debug(
                """
                Registeration failed. Check the URL, header, registration data and that
                server is running.
                """
            )
            global EXIT
            EXIT = True
            _debug('Closing...')
            return None
        _debug(f"Register response: {str(response)}")
        _debug(response.content)
        if response.status_code == 500:
            _debug('Worker already registered')

    def _next_invocation(self) -> Optional[Invocation]:
        """
        Blocks until next invocation is available. Returns invocation spec.
        """
        _debug('Requesting a new invocation')
        try:
            invocation_response = requests.get(
                f"{self._url}/invocations/next",
                headers=self._headers
            )
        except TimeoutError:
            _debug('Could not query for jobs.')
            return None

        # Check response
        if invocation_response.status_code == 401:
            _debug('Detector not authorized')
            return None

        if invocation_response.status_code == 403:
            _debug('Detector removed. Registering again')
            self._register()
            return None

        if invocation_response.status_code == 404:
            _debug('No invocations available')
            return None

        if invocation_response.status_code != 200:
            _debug("Error: code %d" % invocation_response.status_code)
            return None

        return Invocation(invocation_response.json())

    def _fetch_data(self, invocation: Invocation) -> pd.DataFrame:
        """Fetch data."""
        _debug("URL: " + f"{self._url}/invocations/{invocation.invocation_id}/data")
        _debug("Headers: " + str(self._headers))

        response = requests.get(
            f"{self._url}/invocations/{invocation.invocation_id}/data",
            headers=self._headers
        )

        return pd.read_json(response.content)

    def _update_invoke_status(self, status, invocation: Invocation) -> Any:
        """Fetch data."""
        return requests.put(
            f"{self._url}/invocations/{invocation.invocation_id}",
            headers=self._headers,
            data=json.dumps({'status': status}),
        )

    def _upload_timeseries_data(self, data: pd.DataFrame, invocation: Invocation):
        """Upload output columns from DataFrame."""
        if len(self.columns) > 0:
            columns = [
                x
                for x in self._auto_gen_indices()
                if x['columnType'] == 'output' and x['index'] > 0
            ]
        else:
            columns = [
                { 'index': name, 'name': name }
                for i, name in enumerate(data.columns.to_list()[1:])
            ]
        responseData = []
        for _, cast in data.iterrows():
            if hasattr(cast, 'ds'):
                timestamp = cast.ds.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                timestamp = cast.iloc[0]
        

            values = {}
            for column in columns:
                values[str(column['index'])] = cast[column['name']]

            y = {
                'time': timestamp,
                'values': values
            }
            responseData.append(y)

        #_debug(json.dumps(responseData))
        response = requests.post(
            f"{self._url}/invocations/{invocation.invocation_id}/series-result",
            headers=self._headers,
            data=json.dumps(responseData),
        )
        _debug(f"Response code for publishing the data: {str(response.status_code)}")

    def _upload_anomalies(self, invocation: Invocation, anomalies: List[Anomaly]):
        response = requests.post(
            f"{self._url}/invocations/{invocation.invocation_id}/anomaly-result",
            headers=self._headers,
            data=json.dumps(anomalies),
        )
        _debug(f"Response code for publishing the data: {str(response.status_code)}")

    def _upload_data_sample(self, invocation: Invocation, sample: DataSample):
        columnlist = []
        for column in sample.columns:
            columnlist.append(column.dict())
        
        print(columnlist)

        datalist = []

        for _, cast in sample.data.iterrows():
            templist = cast.to_list()
            endlist = []

            for element in templist:
                endlist.append(str(element))
            datalist.append(endlist)

        print(datalist)

        sample = {
            'columns': columnlist,
            'data': datalist,
            'errorFlag': False
        }

        print(sample)

        response = requests.post(
            f"{self._url}/invocations/{invocation.invocation_id}/data-sample",
            headers=self._headers,
            data=json.dumps(sample),
        )
        _debug(f"Response code for publishing the data: {str(response.status_code)}")


class ForecastWorker(QuantaWorker):
    """Forecast Quanta Worker."""

    @property
    def worker_type(self):
        return 'Forecast'

    def invoke(self, invocation: Invocation):
        """Invoke forecast task."""
        data = self._fetch_data(invocation)
        result = self.forecast(data, invocation.parameters)
        self._upload_timeseries_data(result, invocation)

    @abstractmethod
    def forecast(
        self,
        data: pd.DataFrame,
        params: List[InvocationParam]
    ) -> pd.DataFrame:
        """Run forecast."""
        pass


class DetectorWorker(QuantaWorker):
    """Anomaly detection Quanta Worker."""

    @property
    def worker_type(self):
        return 'Detect'

    def invoke(self, invocation: Invocation):
        """Invoke anomaly detector."""
        data = self._fetch_data(invocation)
        result = self.detect(data, invocation.parameters)
        self._upload_anomalies(invocation, result)

    @abstractmethod
    def detect(
        self,
        data: pd.DataFrame,
        params: List[InvocationParam]
    ) -> List[Anomaly]:
        """Run anomaly detection."""
        pass


class ImportWorker(QuantaWorker):
    """Import Quanta Worker."""

    @property
    def worker_type(self):
        return 'Import'

    def invoke(self, invocation: Invocation):
        print(invocation.task_type)
        """Invoke Import Worker."""
        if invocation.task_type == 'IMPORT_SAMPLE':
            sample = self.run_sample(invocation.parameters)
            self._upload_data_sample(invocation, sample)
        else:
            data = self.run_import(invocation.parameters)
            if not data.empty:
                self._upload_timeseries_data(data, invocation)

    @abstractmethod
    def run_sample(self, params: List[InvocationParam]) -> DataSample:
        """Sample data."""
        pass

    @abstractmethod
    def run_import(self, params: List[InvocationParam]) -> pd.DataFrame:
        """Run import."""
        pass
