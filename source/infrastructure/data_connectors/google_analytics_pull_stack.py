# pylint: disable=line-too-long
"""
This module is responsible as the main stack generation entry point.
"""
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

from aws_cdk import CfnParameter
from data_connectors.appflow_pull_stack import AppFlowPullStack


class GoogleAnalyticsPullStack(AppFlowPullStack):
    """
    This class represents the Google Analytics pull connectors stack
    """

    name = "GoogleAnalyticsPullStack"
    description = "Deploy and use a connector for Google Analytics data"
    template_filename = "google-analytics-connector.template"

    def create_schema_provider_parameter(self):
        """
        This function is responsible for creating the schema provider parameter
        This is an override from BaseConnectorStack
        """
        return CfnParameter(
            self,
            "SchemaProviderName",
            allowed_values=["Google Analytics"],
            default="Google Analytics",
        )
