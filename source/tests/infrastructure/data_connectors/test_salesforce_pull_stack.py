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
from data_connectors.salesforce_pull_stack import SalesforceMarketingCloudStack


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent / "cdk.json"
    return CDKSolution(cdk_json_path=path)


@pytest.fixture(scope="module")
def synth_template(mock_solution):
    app = cdk.App(context=mock_solution.context.context)
    stack = SalesforceMarketingCloudStack(
        app,
        SalesforceMarketingCloudStack.name,
        description=SalesforceMarketingCloudStack.description,
        template_filename=SalesforceMarketingCloudStack.template_filename,
    )
    synth_template = Template.from_stack(stack)
    yield synth_template


def test_salesforce_appflow_resource_creation(synth_template):
    synth_template.resource_count_is("AWS::AppFlow::Flow", 1)


def test_lambda_appflow_policies(synth_template):
    template = synth_template
    app_flow_resource_capture = Capture()
    secret_manager_resource_capture = Capture()
    s3_resource_manager_capture = Capture()
    template.has_resource(
        "AWS::IAM::Policy",
        {
            "Properties": {
                "PolicyDocument": {
                    "Statement":[
                        {
                            "Action": [
                                "appflow:CreateConnectorProfile",
                                "appflow:List*",
                                "appflow:Describe*",
                                "kms:List*",
                                "kms:Describe*"
                            ],
                            "Effect": "Allow",
                            "Resource": "*"
                        },
                        {
                            "Action": [
                                "appflow:UpdateConnectorProfile",
                                "appflow:DeleteConnectorProfile"
                            ],
                            "Effect": "Allow",
                            "Resource": app_flow_resource_capture
                        },
                        {
                            "Action": [
                                "secretsmanager:CreateSecret",
                                "secretsManager:PutResourcePolicy",
                                "secretsmanager:PutSecretValue",
                                "secretsmanager:GetSecretValue"
                            ],
                            "Effect": "Allow",
                            "Resource": secret_manager_resource_capture
                        },
                        {
                            "Action": [
                                "s3:putobject",
                                "s3:getbucketacl",
                                "s3:putobjectacl",
                                "s3:abortmultipartupload",
                                "s3:listmultipartuploadparts",
                                "s3:listbucketmultipartuploads"
                            ],
                            "Effect": "Allow",
                            "Resource": s3_resource_manager_capture
                        },
                    ],
                },
            },
        }
    )
