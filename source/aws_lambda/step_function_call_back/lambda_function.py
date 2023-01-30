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
import sys
import json

import shared.stepfunctions as stepfunctions

from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key
from aws_solutions.core.helpers import get_service_resource

logger = Logger(utc=True)
DDB_TABLE_NAME = "DDB_TABLE_NAME"


def verify_env_setup():
    if not (os.environ.get(DDB_TABLE_NAME)):
        err_msg = f"The lambda requires {DDB_TABLE_NAME} environment variable to be configured. One or more of these environment varialbes have not been configured"
        logger.error(err_msg)
        raise ValueError(err_msg)

def handler(event, _):
    verify_env_setup()

    try:
        job_id = event['detail']['jobRunId']
        logger.info(f"Querying the dynamodb table {DDB_TABLE_NAME} to retrieve the token for the following job {job_id}")

        ddb_client = get_service_resource('dynamodb')
        ddb_table = ddb_client.Table(os.environ["DDB_TABLE_NAME"])
        response = ddb_table.query(KeyConditionExpression=Key("job_id").\
                                        eq(job_id))
        task_token = response["Items"][0]["task_token"]
    except Exception as err:
        logger.error(f"The following error were found while querying database to "
                     f"retrieve the task_token ==>> {err}")
        raise err

    logger.info("The Token is found and retreived. Communicating with step function to continue...")
    stepfunctions.send_task_success(json.dumps({"status": "Success",
                                                "job_run_id": job_id}), task_token)
