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
This module is a custom resource Lambda for Transforms responsible for
creating/updating/deleting DataBrew recipe and uploading sample file on create.
"""

import json
import re
from typing import Union
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
@helper.update
def on_create_or_update(event, _) -> None:
    """
    This function handles the create and update of databrew recipe
    """
    resource_properties = event["ResourceProperties"]
    
    recipe_s3_location: str = resource_properties["recipe_s3_location"]
    file_content = recipe_file_content(recipe_s3_location)

    databrew = get_service_client("databrew")
    request_type = event["RequestType"]
    if request_type == "Create":
        upload_sample_file_object(resource_properties)
        
        response: dict[str, str] = databrew.create_recipe(
            Name=resource_properties["recipe_name"], 
            Steps=json.loads(file_content)
        )
        logger.info(f'Created recipe: {response["Name"]}')

    elif request_type == "Update":
        response: dict[str, str] = databrew.update_recipe(
            Name=resource_properties["recipe_name"], 
            Steps=json.loads(file_content)
        )
        logger.info(f'Updated recipe: {response["Name"]}')


@helper.delete
def on_delete(event, _) -> None:
    """
    This function handles the delete
    """
    logger.info(f"Resource marked for deletion: {event['PhysicalResourceId']}")
    resource_properties = event["ResourceProperties"]
    recipe_name = resource_properties["recipe_name"]
    databrew = get_service_client("databrew")
    
    response = databrew.list_recipe_versions(
        Name=recipe_name,
        MaxResults=100
    )
    recipe_versions: list[str] = [recipe_item["RecipeVersion"] for recipe_item in response["Recipes"]]

    # delete any previous recipe versions 
    for version in recipe_versions:
        databrew.delete_recipe_version(
            Name=recipe_name,
            RecipeVersion=version,
        )

    # delete the latest version
    response: dict[str, str] = databrew.delete_recipe_version(
        Name=recipe_name,
        RecipeVersion="LATEST_WORKING",
    )
    logger.info(f'Deleted recipe: {response["Name"]}')


def upload_sample_file_object(resource_properties) -> None:
    s3_client = get_service_client("s3")
    inbound_bucket_name: str = resource_properties["inbound_bucket_name"]
    inbound_bucket_prefix: str = resource_properties["inbound_bucket_prefix"]
    object_key = f"{inbound_bucket_prefix}empty-file-object"

    s3_client.upload_file(
        "empty-file-object", 
        inbound_bucket_name,
        object_key,
    )
    logger.info(f"Uploaded {object_key}")


def recipe_file_content(recipe_s3_location: str):
    if recipe_s3_location:
        (recipe_bucket, recipe_key) = get_bucket_key_from_location(recipe_s3_location)
        logger.info(f"Recipe bucket: {recipe_bucket}, Recipe key: {recipe_key}")
        s3 = get_service_client("s3")
        s3_obj = s3.get_object(
            Bucket=recipe_bucket, 
            Key=recipe_key,
        )
        logger.info("Recipe file contents read, creating recipe")
        return s3_obj["Body"].read().decode("utf-8")
    else:
        logger.info("Recipe file not provided, creating empty recipe")
        return "[]"
    

def get_bucket_key_from_location(recipe_file_location: str) -> tuple[str, Union[str, None]]: 
    pattern: str = r"^(.*?)\/(.*)$"
    # matches (abc)/(def/1-2-3/pqr)

    if match := re.match(pattern, recipe_file_location):
        groups: tuple[str] = match.groups()
        return groups[0], groups[1]

    raise ValueError("Invalid recipe file location format")
    
