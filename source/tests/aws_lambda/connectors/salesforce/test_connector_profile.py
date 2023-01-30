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

from pathlib import Path
import json
import os
import aws_cdk as cdk
import pytest
from unittest.mock import patch, Mock, MagicMock
from aws_cdk.assertions import Template, Capture
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients, _helpers_service_resources
from aws_solutions.cdk import CDKSolution

SECRET_VALUE = {
    'SecretString':
    json.dumps({
        "profile_name": "profile_name",
        "client_id": "client_id",
        "client_secret": "client_secret",
        "token_endpoint": "token_endpoint",
        "instance_url": "instance_url",
    }),
}


def mock_environ():
    """
    This function is the mocked (replaced) function for returning environment variables
    """
    mock_env = {
        "CONNECTOR_SECRET_ARN": "MockSecretARN",
        "AWS_REGION": "us-east-1",
        "SOLUTION_ID": "SO999",
        "SOLUTION_VERSION": "v9.9.9"
    }
    return mock_env


class MockSecretsManager:
    get_secret_value = MagicMock(return_value=SECRET_VALUE)


class MockConnectorProfile:
    create = MagicMock()
    update = MagicMock()
    delete = MagicMock()


@patch('os.environ', new=mock_environ())
@patch('aws_lambda_powertools.Logger', new=MagicMock())
@patch('aws_solutions.core.helpers.get_service_client',
       new=MagicMock(return_value=MockSecretsManager))
def test_get_connector_profile():
    from aws_lambda.connectors.salesforce import connector_profile
    connector_profile.get_connector_profile()


@patch('os.environ', new=mock_environ())
@patch('aws_lambda_powertools.Logger', new=Mock())
@patch(
    'aws_lambda.connectors.salesforce.connector_profile.get_connector_profile',
    new=MagicMock(return_value=MockConnectorProfile))
def test_create_event_handler():
    from aws_lambda.connectors.salesforce import connector_profile
    connector_profile.create_event_handler({}, {})


@patch('os.environ', new=mock_environ())
@patch('aws_lambda_powertools.Logger', new=Mock())
@patch(
    'aws_lambda.connectors.salesforce.connector_profile.get_connector_profile',
    new=MagicMock(return_value=MockConnectorProfile))
def test_update_event_handler():
    from aws_lambda.connectors.salesforce import connector_profile
    connector_profile.update_event_handler({}, {})


@patch('os.environ', new=mock_environ())
@patch('aws_lambda_powertools.Logger', new=Mock())
@patch(
    'aws_lambda.connectors.salesforce.connector_profile.get_connector_profile',
    new=MagicMock(return_value=MockConnectorProfile))
def test_delete_event_handler():
    from aws_lambda.connectors.salesforce import connector_profile
    connector_profile.delete_event_handler({}, {})
