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
from unittest.mock import Mock
from datetime import datetime
from boto3.dynamodb.conditions import Key

from aws_lambda.automatic_brew_job_launch.lambda_function import event_handler, logger as lambda_function_logger
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources


@pytest.fixture(autouse=True)
def mock_env_variables():
    os.environ["DDB_TABLE_NAME"] = "s3_object_create_event_time_keeper"
    os.environ["STATE_MACHINE_ARN"] = "state_machine_arn"
    os.environ["AUTOMATIC_DATABREW_JOB_LAUNCH"] = "ON"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["WAITING_TIME_IN_MINUTES"] = "1"


@pytest.fixture()
def _mock_stepfunctions_client():
    client = get_service_client('stepfunctions')
    client.start_execution = Mock(return_value={'executionArn': 'state_machine_execution_arn',
                                                'startDate': datetime(2022, 1, 1)})
    client.list_executions = Mock(
        return_value={
            'executions': [],
            "nextToken": "any"
        }
    )
    return client


@pytest.fixture()
def mock_dynamodb_and_stepfunctions(monkeypatch, _mock_stepfunctions_client, dynamodb_client):
    monkeypatch.setitem(_helpers_service_clients,
                        'stepfunctions', _mock_stepfunctions_client)
    monkeypatch.setitem(_helpers_service_resources,
                        'dynamodb', dynamodb_client)
    monkeypatch.setattr(lambda_function_logger, 'error', Mock())


@pytest.fixture()
def dynamodb_client():
    with mock_dynamodb():
        ddb = boto3.resource('dynamodb', 'us-east-1')
        table_attr = [
            {
                'AttributeName': 'watching_key',
                'AttributeType': 'S'
            }
        ]
        table_schema = [
            {
                'AttributeName': 'watching_key',
                'KeyType': 'HASH'
            }
        ]
        ddb.create_table(AttributeDefinitions=table_attr,
                         TableName=os.environ["DDB_TABLE_NAME"],
                         KeySchema=table_schema,
                         BillingMode='PAY_PER_REQUEST'
                         )
        yield ddb


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "Records": [
                {
                    "body": "{\"Records\": [{\"eventTime\": \"2022-11-17T16:21:16.974Z\", \"s3\": {\"bucket\": {\"arn\": \"s3_bucket_arn\"}, \"object\": {\"key\": \"file_1\"}}}]}"
                }
            ],
        }
    ],
)
def test_handler_success(lambda_event, mock_dynamodb_and_stepfunctions, dynamodb_client):
    event_handler(lambda_event, None)

    table = dynamodb_client.Table(os.environ["DDB_TABLE_NAME"])
    ts = table.query(KeyConditionExpression=Key("watching_key").eq(
        "s3_bucket_arn"))["Items"][0]["timestamp_str"]

    _helpers_service_clients["stepfunctions"].start_execution.assert_called_once()

    assert ts


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "Records": [
                {
                    "body": "{\"Records\": [{\"eventTime\": \"2022-11-17T16:21:16.974Z\"}]}"
                }
            ],
        }
    ],
)
def test_handler_failure(lambda_event, mock_dynamodb_and_stepfunctions, dynamodb_client):
    with pytest.raises(KeyError, match=r's3'):
        event_handler(lambda_event, None)
