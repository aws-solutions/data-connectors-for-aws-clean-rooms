# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#   Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#   the specific language governing permissions and limitations under the License.                                     #
# ######################################################################################################################

import botocore
from aws_lambda_powertools import Logger
from aws_solutions.core.helpers import get_service_client
from botocore.exceptions import ClientError

logger = Logger(utc=True)


def send_task_success(output: str, task_token: str, service_client: botocore.client.BaseClient = None):
    """Method to send success status with output to stepfunction state"""
    if not service_client:
        service_client = get_service_client("stepfunctions")
    try:
        logger.debug("Sending successful output")
        service_client.send_task_success(output=output, taskToken=task_token)
    except ClientError as error:
        logger.error(
            f"Error ocurred when sending task success status for output: {output} and taskToken: {task_token}. Following error occured: {str(error)}. Sending task failure"
        )
        send_task_failure(error, task_token, service_client=service_client)
        raise error


def send_task_failure(error: Exception, task_token: str, service_client: botocore.client.BaseClient = None):
    """Method to send failure status with error information to stepfunction state"""
    if not service_client:
        service_client = get_service_client("stepfunctions")
    try:
        logger.debug("Sending failure output")
        service_client.send_task_failure(
            cause=getattr(error, "message", str(error)), error=str(error), taskToken=task_token
        )
    except ClientError as cli_error:
        logger.error(
            f"Failure to send error status to stepfunction for taskToken {task_token}. Following error occured {str(cli_error)}"
        )
        raise cli_error


def send_heart_beat(task_token: str, service_client: botocore.client.BaseClient = None):
    if not service_client:
        service_client = get_service_client("stepfunctions")
    try:
        logger.debug("Sending heartbeat")
        service_client.send_task_heartbeat(taskToken=task_token)
    except ClientError as cli_error:
        logger.error(
            f"Error occured when sending heart beat for task {task_token}. Following error occured: {str(cli_error)}. Will not send failure notice"
        )
