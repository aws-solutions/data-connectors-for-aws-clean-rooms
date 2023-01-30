#!/usr/bin/env python3

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

import logging
from pathlib import Path

from aws_cdk import App, Aspects

from aws_solutions.cdk import CDKSolution
from cdk_nag import AwsSolutionsChecks
from data_connectors.salesforce_pull_stack import SalesforceMarketingCloudStack
from data_connectors.s3_push_stack import S3PushStack
from data_connectors.app_registry import AppRegistry

solution = CDKSolution(cdk_json_path=Path(__file__).parent.absolute() / "cdk.json")

logger = logging.getLogger("cdk-helper")


def synthesizer():
    return CDKSolution(
        cdk_json_path=Path(__file__).parent.absolute() / "cdk.json"
    ).synthesizer


BUILD_STACKS = [SalesforceMarketingCloudStack, S3PushStack]


@solution.context.requires("SOLUTION_NAME")
@solution.context.requires("SOLUTION_ID")
@solution.context.requires("SOLUTION_VERSION")
@solution.context.requires("BUCKET_NAME")
def build_app(context):
    app = App(context=context)
    for stack in BUILD_STACKS:
        stk = stack(
            app,
            stack.name,
            description=stack.description,
            template_filename=stack.template_filename,
            synthesizer=synthesizer(),
        )
        Aspects.of(app).add(AwsSolutionsChecks())
        Aspects.of(app).add(AppRegistry(stk, f'AppRegistry-{stack.name}'))
    return app.synth(validate_on_synthesis=True, skip_validation=False)


if __name__ == "__main__":
    build_app()
