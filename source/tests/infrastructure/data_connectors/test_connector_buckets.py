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

from pathlib import Path

import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Template

from aws_solutions.cdk import CDKSolution
from data_connectors.base_connector_stack import BaseConnectorStack


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent / "cdk.json"
    return CDKSolution(cdk_json_path=path)


@pytest.fixture(scope="module")
def synth_template(mock_solution):
    app = cdk.App(context=mock_solution.context.context)
    stack = BaseConnectorStack(
        app,
        "BaseConnectorStack",
        description="Deploy and use a base connector",
        template_filename="base-connector.template",
    )
    synth_template = Template.from_stack(stack)
    yield synth_template


def test_inbound_bucket_creation(synth_template):
    template = synth_template
    template.has_resource(
        "AWS::S3::Bucket",
        {
            "Properties": {
                "LoggingConfiguration": {
                    "LogFilePrefix": "inbound-logs/"
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True
                }
            },
            "UpdateReplacePolicy": "Retain",
            "DeletionPolicy": "Retain"
        }
    )


def test_transform_bucket_creation(synth_template):
    template = synth_template
    template.has_resource(
        "AWS::S3::Bucket",
        {
            "Properties": {
                "LoggingConfiguration": {
                    "LogFilePrefix": "transform-logs/"
                },
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "BlockPublicPolicy": True,
                    "IgnorePublicAcls": True,
                    "RestrictPublicBuckets": True
                }
            },
            "UpdateReplacePolicy": "Retain",
            "DeletionPolicy": "Retain"
        }
    )


def test_iam_group_creation(synth_template):
    template = synth_template
    template.has_resource("AWS::IAM::Group", {})


def test_iam_group_policy(synth_template):
    template = synth_template
    template.has_resource_properties(
        "AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": [{
                    "Action":
                    ["s3:put*", "s3:get*", "s3:list*", "s3:*multipart*"],
                    "Effect":
                    "Allow",
                }, {
                    "Action": [
                        "kms:encrypt*", "kms:list*", "kms:get*",
                        "kms:generate*", "kms:describe*"
                    ],
                    "Effect":
                    "Allow",
                    "Resource":
                    "*"
                }],
                "Version":
                "2012-10-17"
            },
        })
