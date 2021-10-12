"""Invocation tests."""
import pytest

from quantaworker import Invocation

def test_dict_conversion():
    # invocation_dict = {
    #     'invocationId': 1,
    #     'task': {
    #         'taskType': 'import'
    #     },
    #     'parameters': [{
    #         'name': 'test_1',
    #         'value': '10.0'
    #     }]
    # }

    invocation_dict = {
        'invocationId': 1,
        'parameters': [{
            'name': 'test_1',
            'value': '10.0'
        }]
    }
    
    invocation = Invocation(invocation_dict)
    assert invocation.invocation_id == 1
    # assert invocation.task_type == 'import'
    
    params = invocation.parameters
    assert len(params) == 1

    assert params[0].name == "test_1"
    assert params[0].value == "10.0"
