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
from datetime import datetime, timedelta, timezone
from aws_solutions.core.helpers import get_service_client, get_service_resource
from aws_lambda_powertools import Logger

logger = Logger(utc=True, service="sfmc-lambda-standalone")

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
WAITING_TIME_IN_SECONDS = 60
EXPECTED_FINISH_TIME_DELTA = 0

STATE_MACHINE_ARN = "STATE_MACHINE_ARN"
DDB_TABLE_NAME = "DDB_TABLE_NAME"
AUTOMATIC_DATABREW_JOB_LAUNCH = "AUTOMATIC_DATABREW_JOB_LAUNCH"


def verify_env_setup():
    if not (os.environ.get(DDB_TABLE_NAME) and os.environ.get(STATE_MACHINE_ARN)):
        err_msg = f"The lambda requires {DDB_TABLE_NAME} and {STATE_MACHINE_ARN} environment variables to be configured." \
                  f" One or more of these environment variables have not been configured"
        logger.error(err_msg)
        raise ValueError(err_msg)


def format_timestamp(timestamp):
    """
    Timestamps must conform to RFC3339 profile ISO 8601 for stepfunctions choice.
    """
    return timestamp.strftime(TIMESTAMP_FORMAT)


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


def extract_record_info(record):
    bucket_name = record['s3']['bucket']['arn']
    event_time = record['eventTime']
    return bucket_name, event_time


def invoke_state_machine(watching_key, utc_timestamp_in_str, delayed_sec, time_delta):
    stepfunctions_client = get_service_client("stepfunctions")

    expected_upload_finish_time = datetime.strptime(utc_timestamp_in_str, TIMESTAMP_FORMAT) + timedelta(
        seconds=time_delta)
    expected_upload_finish_time_str = format_timestamp(expected_upload_finish_time)

    state_machine_input = {
        "watching_key": watching_key,
        "waiting_time_in_seconds": delayed_sec,
        "expected_upload_finish_time_str": expected_upload_finish_time_str
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
    logger.info(
        f"Compare current event timestamp {ts} and timestamp in dynamodb {current_timestamp_in_dynamodb}")
    return ts > current_timestamp_in_dynamodb


def get_watching_key(event):
    unique_watching_keys = set()
    for record in event['Records']:
        bucket_name, event_time = extract_record_info(record)
        logger.debug(f'Processing new file {record["s3"]["object"]} upload to {bucket_name} at {event_time}')
        unique_watching_keys.add(bucket_name)

    watching_key = ';'.join(sorted(unique_watching_keys))
    return watching_key


def event_handler(event, _):
    verify_env_setup()

    if os.environ[AUTOMATIC_DATABREW_JOB_LAUNCH] == "OFF":
        logger.info("Automatic Databrew job launch is off")
    else:
        try:
            watching_key = get_watching_key(event)

            ts = datetime.now(timezone.utc)
            ts_in_str = format_timestamp(ts)

            dynamodb_client = get_service_resource("dynamodb")
            dynamodb_table = dynamodb_client.Table(os.environ[DDB_TABLE_NAME])

            item_in_dynamodb = get_timestamp(dynamodb_table, watching_key)

            if not item_in_dynamodb or has_newer_timestamp(ts_in_str, item_in_dynamodb["timestamp_str"]):
                put_timestamp(dynamodb_table, watching_key, ts_in_str)
                logger.info(
                    f'Lambda finished processing object create notification for {watching_key} at event time [{ts_in_str}]')

                response = invoke_state_machine(watching_key, ts_in_str, WAITING_TIME_IN_SECONDS,
                                                EXPECTED_FINISH_TIME_DELTA)
            else:
                logger.info("Not invoking state machine due to not newer event timestamp")
                return

        except Exception as err:
            logger.error(err)
            raise err

        return {"automatic_brew_job_launch_execution": response["executionArn"]}
