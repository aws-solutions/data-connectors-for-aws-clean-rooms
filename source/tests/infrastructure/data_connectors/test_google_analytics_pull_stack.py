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

from data_connectors.google_analytics_pull_stack import GoogleAnalyticsPullStack
from pathlib import Path

import aws_cdk as cdk
from aws_solutions.cdk import CDKSolution
import pytest
from aws_cdk.assertions import Template


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent / "cdk.json"
    return CDKSolution(cdk_json_path=path)


@pytest.fixture(scope="module")
def synth_template(mock_solution):
    app = cdk.App(context=mock_solution.context.context)
    stack = GoogleAnalyticsPullStack(
        app,
        GoogleAnalyticsPullStack.name,
        description=GoogleAnalyticsPullStack.description,
        template_filename=GoogleAnalyticsPullStack.template_filename,
    )
    synth_template = Template.from_stack(stack)
    yield synth_template


def test_create_schema_provider_parameter(synth_template):
    template = synth_template
    template.has_parameter(
        "SchemaProviderName",
        {
            "Default": "Google Analytics",
            "AllowedValues": [
                "Google Analytics"
            ]
        }
    )
