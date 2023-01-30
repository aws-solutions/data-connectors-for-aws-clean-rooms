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


def get_secret_value(secret_name):
    service_client = get_service_client("secretsmanager")
    try:
        service_client.get_secret_value(SecretId=secret_name)
    except ClientError as error:
        logger.error(f"Error occured when trying secret: {secret_name}. Following error occured: {str(error)}")
        raise error
