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
from aws_cdk.assertions import Match, Template, Capture
from aws_solutions.cdk import CDKSolution
from aws_solutions.cdk.stack import SolutionStack
from data_connectors.orchestration.async_callback_construct import AsyncCallbackConstruct


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent / ".." / "cdk.json"
    return CDKSolution(cdk_json_path=path)


workflow_name = "UnitTestWorkflow"


@pytest.fixture(scope="module")
def synth_template(mock_solution):
    app = cdk.App(context=mock_solution.context.context)
    stack = SolutionStack(app,
                          "TestAsyncCallback",
                          description="Empty Stack for Testing",
                          template_filename="test-async-callback.template")

    AsyncCallbackConstruct(stack, "TestAsyncCallback", job_name="UnitTestJob", workflow_name=workflow_name)
    synth_template = Template.from_stack(stack)
    yield synth_template


def test_lambda_dbb_function_creation(synth_template):
    ddb_defintion_capture = Capture()
    role_definition_capture = Capture()
    synth_template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Handler": "lambda_function.handler",
            "Description":
                "This function read dynamodb table and send token back to step function",
            "Role": {
                "Fn::GetAtt": [role_definition_capture, "Arn"]
            },
            "Environment": {
                "Variables": {
                    "DDB_TABLE_NAME": {
                        "Ref": ddb_defintion_capture
                    },
                }
            },
        },
    )

    synth_template.has_resource_properties(
        "AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": [{
                    "Action": [
                        "dynamodb:BatchWriteItem", "dynamodb:PutItem",
                        "dynamodb:UpdateItem", "dynamodb:DeleteItem",
                        "dynamodb:DescribeTable"
                    ],
                    "Effect":
                        "Allow",
                }],
                "Version":
                    "2012-10-17"
            },
        })

    synth_template.has_resource_properties(
        "AWS::IAM::Policy", {
            "PolicyDocument": {
                "Statement": [{
                    "Action": [
                        "dynamodb:BatchGetItem", "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator", "dynamodb:Query",
                        "dynamodb:GetItem", "dynamodb:Scan",
                        "dynamodb:ConditionCheckItem", "dynamodb:DescribeTable"
                    ],
                    "Effect":
                        "Allow",
                }],
                "Version":
                    "2012-10-17"
            },
        })

    synth_template.has_resource_properties(
        "AWS::DynamoDB::Table",
        {
            "KeySchema": [{
                "AttributeName": "job_id",
                "KeyType": "HASH"
            }],
            "SSESpecification": {
                "SSEEnabled": True
            },
            "AttributeDefinitions": [{
                "AttributeName": "job_id",
                "AttributeType": "S"
            }],
            "TimeToLiveSpecification": {
                "AttributeName": "exp_timestamp",
                "Enabled": True,
            },
        },
    )
    assert synth_template.to_json()["Resources"][
               ddb_defintion_capture.as_string()]["Type"] == "AWS::DynamoDB::Table"


def test_lambda_brew_run_policy(synth_template):
    resource_capture = Capture()
    synth_template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": [{
                    "Action":
                        ["states:SendTaskFailure", "states:SendTaskHeartbeat"],
                    "Effect": "Allow",
                    "Resource": resource_capture
                }, {
                    "Action": "databrew:*",
                    "Effect": "Allow",
                }, {
                    "Action": "dynamodb:PutItem",
                    "Effect": "Allow",
                }],
                "Version":
                    "2012-10-17"
            },
        },
    )

    assert workflow_name in str(resource_capture.as_object().values())


def test_lambda_callback_policy(synth_template):
    resource_capture = Capture()
    synth_template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement":
                    Match.array_with([{
                        "Action": [
                            "states:SendTaskFailure",
                            "states:SendTaskSuccess",
                        ],
                        "Effect": "Allow",
                        "Resource": resource_capture,
                    }]),
                "Version":
                    "2012-10-17",
            }
        },
    )
    assert workflow_name in str(resource_capture.as_object().values())


def test_trigger_lambda_after_brew(synth_template):
    synth_template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "EventPattern": {
                "source": ["aws.databrew"]
            },
            "State": "ENABLED",
        },
    )
