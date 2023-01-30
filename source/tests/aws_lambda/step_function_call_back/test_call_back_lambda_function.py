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
from aws_lambda.step_function_call_back.lambda_function import handler, logger as lambda_function_logger
from aws_solutions.core.helpers import _helpers_service_resources
import shared.stepfunctions as stepfunctions


@pytest.fixture(autouse=True)
def mock_env_variables():
    os.environ["DDB_TABLE_NAME"] = "StepFunctionTaskTokenTable"
    os.environ["AWS_REGION"] = "us-east-1"


@pytest.fixture()
def mock_stepfunctions(monkeypatch, dynamodb_client):
    monkeypatch.setitem(_helpers_service_resources, 'dynamodb', dynamodb_client)
    monkeypatch.setattr(stepfunctions, 'send_task_success', Mock())
    monkeypatch.setattr(lambda_function_logger, 'error', Mock())


@pytest.fixture()
def dynamodb_client():
    with mock_dynamodb():
        ddb = boto3.resource("dynamodb", "us-east-1")
        table_attr = [
            {
                'AttributeName': 'job_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'task_token',
                'AttributeType': 'S'
            }]
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
        {"detail": {"jobRunId": "ids"}}
    ],
)
def test_handler(caplog, lambda_event, mock_stepfunctions, dynamodb_client):
    ddb_table = dynamodb_client.Table(os.environ["DDB_TABLE_NAME"])
    ddb_table.put_item(Item={"job_id": "ids",
                             "task_token": "task_token"})
    handler(lambda_event, None)
    assert "Token is found" in caplog.text
    stepfunctions.send_task_success.assert_called_once_with('{"status": "Success", "job_run_id": "ids"}', 'task_token')
