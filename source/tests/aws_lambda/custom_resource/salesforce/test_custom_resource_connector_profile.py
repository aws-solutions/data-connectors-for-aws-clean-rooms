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

import json
from unittest.mock import patch, MagicMock

PROFILE_DICT = {
    "profile_name": "profile_name",
    "client_id": "client_id",
    "client_secret": "client_secret",
    "token_endpoint": "token_endpoint",
    "instance_url": "instance_url",
}

SECRET_VALUE = {
    'SecretString': json.dumps(PROFILE_DICT),
}


class MockConnectorProfile:
    create = MagicMock()
    update = MagicMock()
    delete = MagicMock()


def get_mock_event():
    return {
        "ResourceProperties": {
            "profile_name": "profile_name",
            "client_id": "client_id",
            "client_secret": "client_secret",
            "token_endpoint": "token_endpoint",
            "instance_url": "instance_url",
        }
    }


def test_connector_profile_from_event():
    from aws_lambda.custom_resource.salesforce.connector_profile import connector_profile_from_event
    profile = connector_profile_from_event(get_mock_event())
    assert profile is not None


@patch('aws_lambda_powertools.Logger', new=MagicMock())
def test_custom_resource_create():
    with patch(
            'aws_lambda.custom_resource.salesforce.connector_profile.connector_profile_from_event',
            new=MagicMock(return_value=(MockConnectorProfile, PROFILE_DICT))):
        from aws_lambda.custom_resource.salesforce.connector_profile import custom_resource_create
        custom_resource_create(get_mock_event(), None)


@patch('aws_lambda_powertools.Logger', new=MagicMock())
def test_custom_resource_update():
    with patch(
            'aws_lambda.custom_resource.salesforce.connector_profile.connector_profile_from_event',
            new=MagicMock(return_value=(MockConnectorProfile, PROFILE_DICT))):
        from aws_lambda.custom_resource.salesforce.connector_profile import custom_resource_update
        custom_resource_update(get_mock_event(), None)


@patch('aws_lambda_powertools.Logger', new=MagicMock())
def test_custom_resource_delete():
    with patch(
            'shared.connectors.salesforce.connector.SalesforceConnectorProfile',
            new=MagicMock(return_value=(MockConnectorProfile, PROFILE_DICT))):
        from aws_lambda.custom_resource.salesforce.connector_profile import custom_resource_delete
        custom_resource_delete(get_mock_event(), None)
