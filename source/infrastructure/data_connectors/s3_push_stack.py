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

from data_connectors.base_connector_stack import BaseConnectorStack
from constructs import Construct
from cdk_nag import NagSuppressions


class S3PushStack(BaseConnectorStack):
    """
    This class represents the base S3 push connectors stack
    """

    name = "S3PushStack"
    description = "Deploy and use a connector for data pushed to an S3 bucket"
    template_filename = "simple-storage-service-push-connector.template"

    def __init__(self, scope: Construct, construct_id: str, *args,
                 **kwargs) -> None:
        # parent constructor
        super().__init__(scope, construct_id, *args, **kwargs)
        self.add_cdk_nag_suppressions()

    def add_cdk_nag_suppressions(self):
        for path in [
            "/S3PushStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource",
        ]:
            NagSuppressions.add_resource_suppressions_by_path(
                self,
                path,
                [
                    {
                        "id": "AwsSolutions-IAM4",
                        "reason": "Wildcards are required for related actions",
                    },
                ],
            )

        for path in [
            "/S3PushStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/DefaultPolicy/Resource",
        ]:
            NagSuppressions.add_resource_suppressions_by_path(
                self,
                path,
                [
                    {
                        "id": "AwsSolutions-IAM5",
                        "reason": "Wildcards are required for related actions",
                    },
                ],
            )
