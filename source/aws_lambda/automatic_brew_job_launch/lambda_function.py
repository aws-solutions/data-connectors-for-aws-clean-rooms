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
import os
from botocore.exceptions import ClientError
from aws_solutions.core.helpers import get_service_client, get_service_resource
from aws_lambda_powertools import Logger

logger = Logger(utc=True, service="sfmc-lambda-standalone")

WAITING_TIME_IN_MINUTES = "WAITING_TIME_IN_MINUTES"
STATE_MACHINE_ARN = "STATE_MACHINE_ARN"
DDB_TABLE_NAME = "DDB_TABLE_NAME"
AUTOMATIC_DATABREW_JOB_LAUNCH = "AUTOMATIC_DATABREW_JOB_LAUNCH"

EXPECTED_FINISH_TIME_DELTA = 0


def event_handler(event, _):
    verify_env_setup()

    if os.environ[AUTOMATIC_DATABREW_JOB_LAUNCH] == "OFF":
        logger.info("AutotriggerTransform is OFF")
    else:

        dynamodb_client = get_service_resource("dynamodb")
        dynamodb_table = dynamodb_client.Table(os.environ[DDB_TABLE_NAME])

        stepfunctions_client = get_service_client("stepfunctions")

        try:
            watching_key, ts_in_str = get_watching_key(event)

            item_in_dynamodb = get_timestamp(dynamodb_table, watching_key)
            if not item_in_dynamodb or has_newer_timestamp(ts_in_str, item_in_dynamodb['timestamp_str']):
                put_timestamp(dynamodb_table, watching_key, ts_in_str)
                logger.info(
                    f'Lambda finished processing object create notifications for {watching_key} at latest event time {ts_in_str}')

                running_executions = get_executions(stepfunctions_client, status_filter="RUNNING")
                if not running_executions:
                    response = invoke_state_machine(stepfunctions_client, watching_key)
                else:
                    logger.info("Not executing state machine: State machine is already running")
                    return
            else:
                logger.info("Not executing state machine: There is no newer event timestamp")
                return

        except Exception as err:
            logger.error(err)
            raise err

        return {"automatic_brew_job_launch_execution": response["executionArn"]}


def get_executions(stepfunctions_client, status_filter="RUNNING"):
    state_machine_arn = os.environ[STATE_MACHINE_ARN]
    running_executions = stepfunctions_client.list_executions(stateMachineArn=state_machine_arn,
                                                              statusFilter=status_filter)
    return running_executions['executions']


def verify_env_setup():
    if not (os.environ.get(DDB_TABLE_NAME) and os.environ.get(STATE_MACHINE_ARN)):
        err_msg = f"The lambda requires {DDB_TABLE_NAME} and {STATE_MACHINE_ARN} environment variables to be configured." \
                  f" One or more of these environment variables have not been configured"
        logger.error(err_msg)
        raise ValueError(err_msg)


def put_timestamp(dynamodb_table, watching_key, timestamp_str):
    try:
        dynamodb_table.put_item(
            Item={
                'watching_key': watching_key,
                'timestamp_str': timestamp_str,
            }
        )
        logger.info(f"Update the latest S3 object create event time {timestamp_str} in the {dynamodb_table}")
    except ClientError as error:
        logger.error(error)
        raise error


def extract_s3_record_info(record):
    bucket_name = record['s3']['bucket']['arn']
    file_name = record['s3']['object']['key']
    event_time = record['eventTime']
    return bucket_name, file_name, event_time


def invoke_state_machine(stepfunctions_client, watching_key):
    delayed_sec = int(60 * float(os.environ[WAITING_TIME_IN_MINUTES]))
    state_machine_input = {
        "watching_key": watching_key,
        "waiting_time_in_seconds": delayed_sec,
    }
    state_machine_input_str = json.dumps(state_machine_input)

    state_machine_arn = os.environ[STATE_MACHINE_ARN]

    logger.info(f'Invoking automatic brew job launch workflow {state_machine_arn} with input {state_machine_input_str}')

    return stepfunctions_client.start_execution(
        stateMachineArn=state_machine_arn,
        input=state_machine_input_str
    )


def get_timestamp(dynamodb_table, watching_key):
    response = dynamodb_table.get_item(
        Key={'watching_key': watching_key},
    )
    return response.get('Item', "")


def has_newer_timestamp(ts, current_timestamp_in_dynamodb):
    logger.info(f"Compare current event timestamp {ts} and timestamp in dynamodb {current_timestamp_in_dynamodb}")
    return ts > current_timestamp_in_dynamodb


def get_watching_key(event):
    unique_watching_keys = set()
    latest_file_uploaded_timestamp = ""
    for record in event['Records']:
        payload = json.loads(record["body"])
        for s3_info in payload.get("Records", {}):
            bucket_name, file_name, event_time = extract_s3_record_info(s3_info)
            logger.info(f'Processing new file {file_name} upload to {bucket_name} at {event_time}')
            unique_watching_keys.add(bucket_name)
            latest_file_uploaded_timestamp = max(latest_file_uploaded_timestamp, event_time)

    watching_key = ';'.join(sorted(unique_watching_keys))
    return watching_key, latest_file_uploaded_timestamp
