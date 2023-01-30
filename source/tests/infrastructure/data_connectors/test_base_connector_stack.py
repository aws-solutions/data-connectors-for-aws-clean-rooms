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
from aws_cdk.assertions import Template, Capture

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


def test_sns_notification_parameter(synth_template):
    template = synth_template  # synth_template() included into the test coverage.
    template.has_parameter(
        "NotificationEmail",
        {"Type": "String",
         "Default": "",
         "AllowedPattern": "(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$|^$)",
         "ConstraintDescription": "Must be a valid email address or blank",
         "Description": "Email to notify with Orchestration results",
         "MaxLength": 50
         })


def test_sns_notification_subscription(synth_template):
    template = synth_template
    ref_capture = Capture()
    template.resource_count_is("AWS::SNS::Subscription", 1)
    template.has_resource_properties(
        "AWS::SNS::Subscription",
        {
            "Protocol": "email",
            "TopicArn": {
                "Ref": ref_capture
            },
            "Endpoint": {
                "Ref": "NotificationEmail"
            }
        }
    )


def test_sns_notification_topic(synth_template):
    template = synth_template
    template.resource_count_is("AWS::SNS::Topic", 1)
    template.has_resource(
        "AWS::SNS::Topic",
        {
            "Metadata": {
                "cdk_nag": {
                    "rules_to_suppress": [
                        {
                            "reason": " The SNS Topic does not require publishers to use SSL",
                            "id": "AwsSolutions-SNS3"
                        }
                    ]
                }
            }
        }
    )
