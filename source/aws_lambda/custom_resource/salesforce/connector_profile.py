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
This module is a custom resource Lambda responsible for
working with a Salesforce Marketing Cloud connection in Appflow
"""

from aws_lambda_powertools import Logger
from crhelper import CfnResource
from shared.connectors.salesforce.connector import SalesforceConnectorProfile

logger = Logger(utc=True, service="sfmc-lambda-custom-resource")
helper = CfnResource(log_level="ERROR", boto_level="ERROR")


def connector_profile_from_event(event):
    """
    Helper to remove duplicate code
    """
    properties = event["ResourceProperties"]
    connector = SalesforceConnectorProfile(
        properties["profile_name"],
        properties["client_id"],
        properties["client_secret"],
        properties["token_endpoint"],
        properties["instance_url"],
    )
    return (connector, properties["profile_name"])


@helper.create
def custom_resource_create(event, _):
    """
    This function handles the create message from cloudformation
    """
    # use parameters passed from the custom resource
    connector, profile_name = connector_profile_from_event(event)
    connector.create()
    # return the profile's name as the physical id
    return profile_name


@helper.update
def custom_resource_update(event, _):
    """
    This function handles the update message from cloudformation
    """
    # use parameters passed from the custom resource
    connector, profile_name = connector_profile_from_event(event)
    connector.update()
    # return the profile's name as the physical id
    return profile_name


@helper.delete
def custom_resource_delete(event, _):
    """
    This function handles the delete message from cloudformation
    """
    # fail silently on delete
    try:
        # physical id is the connector profile name
        profile_name = event["PhysicalResourceId"]
        connector = SalesforceConnectorProfile(profile_name)
        connector.delete()
    except Exception as error:
        logger.error(error)


def event_handler(event, context):
    """
    This is the Lambda custom resource entry point.
    """
    helper(event, context)
