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
This module is a custom lambda to remove empty file object place in the inbound bucket
"""

from aws_lambda_powertools import Logger
from crhelper import CfnResource
from aws_solutions.core.helpers import get_service_client

logger = Logger(utc=True, service='transform-custom-lambda')
helper = CfnResource(log_level="ERROR", boto_level="ERROR")


def event_handler(event, context):
    """
    This is the Lambda custom resource entry point.
    """
    logger.info(event)
    helper(event, context)


@helper.create
def on_create_or_update(event, _) -> None:
    resource_properties = event["ResourceProperties"]
    s3_client = get_service_client("s3")
    inbound_bucket_name: str = resource_properties["inbound_bucket_name"]
    inbound_bucket_prefix: str = resource_properties["inbound_bucket_prefix"]
    object_key = f"{inbound_bucket_prefix}empty-file-object"

    try:
        s3_client.delete_object(
            Bucket=inbound_bucket_name,
            Key=object_key
        )
        logger.info(f"Deleted {object_key}")
    except Exception as ex:
        logger.exception(ex)
