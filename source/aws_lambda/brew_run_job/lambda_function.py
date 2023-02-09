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
import time
import shared.stepfunctions as stepfunctions

from datetime import datetime, timedelta
from aws_lambda_powertools import Logger
from aws_solutions.core.helpers import get_service_client, get_service_resource

logger = Logger(utc=True)

DDB_TABLE_NAME = "DDB_TABLE_NAME"
TTL_EXPIRY_PERIOD = 7


def verify_env_setup():
    if not (os.environ.get(DDB_TABLE_NAME)):
        err_msg = f"The lambda requires {DDB_TABLE_NAME} environment variable to be configured. One or more of these environment varialbes have not been configured"
        logger.error(err_msg)
        return False
    return True


def handler(event, _):
    task_token = event["task_token"]
    exp_time = int((timedelta(days=TTL_EXPIRY_PERIOD) + datetime.fromtimestamp(int(time.time()))).timestamp())
    if not verify_env_setup():
        error = ValueError("Cannot Verify the environment Variables for the lambda")
        stepfunctions.send_task_failure(error, task_token)
        raise error

    stepfunctions.send_heart_beat(task_token)

    try:

        data_brew_client = get_service_client("databrew")
        # dynamo db client
        ddb_client = get_service_resource("dynamodb")
        ddb_table = ddb_client.Table(os.getenv("DDB_TABLE_NAME"))
        job_name = event["brew_job_name"]
        response = data_brew_client.start_job_run(Name=job_name)
        job_id = response["RunId"]
        ddb_table.put_item(Item={"job_id": job_id,
                                 "task_token": task_token,
                                 "exp_timestamp": exp_time})

        logger.info(f"Task token of the following databrew job '{job_name}' with job id '{job_id}'"
                    f" is updated in the following {DDB_TABLE_NAME}")

    except Exception as err:
        logger.error(err)
        stepfunctions.send_task_failure(err, task_token)
        raise err

    stepfunctions.send_heart_beat(task_token)

    return {"brew_job_run_id": response["RunId"]}
