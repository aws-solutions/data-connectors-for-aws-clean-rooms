# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################

import os
import boto3
import pytest
from moto import mock_dynamodb
from unittest.mock import Mock, call
from boto3.dynamodb.conditions import Key
import time

from brew_run_job.lambda_function import handler, logger as lambda_function_logger, TTL_EXPIRY_PERIOD
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources
import shared.stepfunctions as stepfunctions


@pytest.fixture(autouse=True)
def mock_env_variables():
    os.environ["DDB_TABLE_NAME"] = "StepFunctionTaskTokenTable"
    os.environ["AWS_REGION"] = "us-east-1"


@pytest.fixture()
def _mock_databrew_client():
    client = get_service_client('databrew')
    client.start_job_run = Mock(return_value={'RunId': 'ids'})
    return client


@pytest.fixture()
def mock_databrew_and_stepfunctions(monkeypatch, _mock_databrew_client, dynamodb_client):
    monkeypatch.setitem(_helpers_service_clients, 'databrew', _mock_databrew_client)
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', dynamodb_client)
    monkeypatch.setattr(stepfunctions, 'send_heart_beat', Mock())
    monkeypatch.setattr(stepfunctions, 'send_task_failure', Mock())
    monkeypatch.setattr(lambda_function_logger, 'error', Mock())


@pytest.fixture()
def dynamodb_client():
    with mock_dynamodb():
        ddb = boto3.resource('dynamodb', 'us-east-1')
        table_attr = [
            {
                'AttributeName': 'job_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'task_token',
                'AttributeType': 'S'
            },
        ]
        table_schema = [
            {
                'AttributeName': 'job_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'task_token',
                'KeyType': 'RANGE'
            },
        ]
        ddb.create_table(AttributeDefinitions=table_attr,
                         TableName=os.environ["DDB_TABLE_NAME"],
                         KeySchema=table_schema,
                         BillingMode='PAY_PER_REQUEST')
        yield ddb


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "task_token": "faketoken",
            "brew_job_name": "Job-Name",
        }
    ],
)
def test_handler_success(lambda_event, mock_databrew_and_stepfunctions, dynamodb_client):
    response = handler(lambda_event, None)
    assert response == {'brew_job_run_id': 'ids'}
    table = dynamodb_client.Table(os.environ["DDB_TABLE_NAME"])
    token = table.query(KeyConditionExpression=Key("job_id").eq("ids"))["Items"][0]["task_token"]
    assert token == "faketoken"
    _helpers_service_clients["databrew"].start_job_run.assert_called_once()
    stepfunctions.send_heart_beat.assert_has_calls([call('faketoken'), call('faketoken')])


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "task_token": "faketoken",
            "brew_job_name": "Job-Name",
        }
    ],
)
def test_exp_time(lambda_event, mock_databrew_and_stepfunctions, dynamodb_client):
    current_time = int(time.time())
    handler(lambda_event, None)
    table = dynamodb_client.Table(os.environ["DDB_TABLE_NAME"])
    exp_time = table.query(KeyConditionExpression=Key("job_id").eq("ids"))["Items"][0]['exp_timestamp']
    ttl_expiry_period_in_seconds = TTL_EXPIRY_PERIOD * 24 * 60 * 60
    assert (exp_time - current_time) == ttl_expiry_period_in_seconds


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "task_token": "faketoken",
        }
    ],
)
def test_handler_failure(lambda_event, mock_databrew_and_stepfunctions, dynamodb_client):
    with pytest.raises(KeyError, match=r'brew_job_name'):
        handler(lambda_event, None)
    stepfunctions.send_task_failure.assert_called_once()
