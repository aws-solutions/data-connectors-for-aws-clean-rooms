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
"""
This module is the Lambda responsible for updating a Salesforce Marketing Cloud connection
"""
import json
import os
from aws_lambda_powertools import Logger
from aws_solutions.core.helpers import get_service_client

from shared.connectors.salesforce.connector import SalesforceConnectorProfile

CONNECTOR_SECRET_ARN = os.environ["CONNECTOR_SECRET_ARN"]

logger = Logger(utc=True, service="sfmc-lambda-standalone")


class ConnectorProfileFunctionException(Exception):
    """
    This is a concrete subclass for exceptions in this module
    """

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


def get_connector_profile():
    # get the secret from the supplied arn
    secretsmanager_client = get_service_client("secretsmanager")
    connection = json.loads(
        secretsmanager_client.get_secret_value(SecretId=CONNECTOR_SECRET_ARN)[
            "SecretString"
        ]
    )
    profile = SalesforceConnectorProfile(
        connection["profile_name"],
        connection["client_id"],
        connection["client_secret"],
        connection["token_endpoint"],
        connection["instance_url"],
    )
    return profile


def create_event_handler(event, _):
    """
    This function is the entry point for Lambda function execution
    """
    try:
        logger.info(json.dumps(event, default=str))
        # get the connection configuration
        profile = get_connector_profile()
        return profile.create()

    except Exception as error:
        # log it and continue bubbling
        logger.error(error)
        raise error


def update_event_handler(event, _):
    """
    This function is the entry point for Lambda function execution
    """
    try:
        logger.info(json.dumps(event, default=str))
        # get the connection configuration
        profile = get_connector_profile()
        return profile.update()

    except Exception as error:
        # log it and continue bubbling
        logger.error(error)
        raise error


def delete_event_handler(event, _):
    """
    This function is the entry point for Lambda function execution
    """
    try:
        logger.info(json.dumps(event, default=str))
        # get the connection configuration
        profile = get_connector_profile()
        return profile.delete()

    except Exception as error:
        # log it and continue bubbling
        logger.error(error)
        raise error
