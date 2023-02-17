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

from aws_cdk import (
    Aws,
    Fn,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks
)
from aws_cdk.aws_logs import LogGroup
from cdk_nag import NagSuppressions
from constructs import Construct

from data_connectors.orchestration.stepfunctions.workflow_orchestrator import WorkflowOrchestrator


class SalesforceWorkflow(WorkflowOrchestrator):
    """Ingests reddit records into S3"""

    def __init__(
            self,
            scope: Construct,
            id: str,
            connector_update_lambda,
            appflow_resource,
            appflow_launch_state_machine_name,
            *args
    ):
        self.connector_update_lambda = connector_update_lambda
        self.appflow_resource = appflow_resource
        self.appflow_launch_state_machine_name = appflow_launch_state_machine_name
        super().__init__(scope, id, *args)

        log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-Appflow-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"

        self.appflow_launch_state_machine = sfn.StateMachine(
            self,
            "SalesforceAppflowLaunch",
            tracing_enabled=True,
            state_machine_name=appflow_launch_state_machine_name,
            definition=self.appflow_launch_definitions,
            logs=sfn.LogOptions(
                level=sfn.LogLevel.ALL,
                destination=LogGroup(self, 'SFNSalesforceAppflowLaunchLogGroup', log_group_name=log_group_name))
        )

        self.salesforce_workflow_cdk_nag_suppression()

    @property
    def appflow_launch_definitions(self) -> sfn.Chain:
        """
        Get the Chain of steps that will accommodate the reddit ingestion
        :return: the Chain of steps
        """
        start_appflow = tasks.CallAwsService(
            self, "StartAppflow",
            service="appflow",
            action="startFlow",
            parameters={
                "FlowName": self.appflow_resource.flow_name,
            },
            iam_resources=[
                f"arn:{Aws.PARTITION}:appflow:{Aws.REGION}:{Aws.ACCOUNT_ID}:flow/{self.appflow_resource.flow_name}",
            ],
        )

        return sfn.Chain.start(self.invoke_connector_update_lambda().next(start_appflow))

    def invoke_connector_update_lambda(self):
        return tasks.LambdaInvoke(self, 'ConnectorUpdate',
                                  lambda_function=self.connector_update_lambda,
                                  payload=sfn.TaskInput.from_object({}))

    def salesforce_workflow_cdk_nag_suppression(self):
        NagSuppressions.add_resource_suppressions(
            self.appflow_launch_state_machine.role.node.try_find_child("DefaultPolicy").node.find_child("Resource"),
            [
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": '* Resources will be suppressed by cdk nag and it has to be not suppressed',
                    "appliesTo": [
                        'Resource::*',
                        'Resource::<ConnectorUpdateFunction80A21979.Arn>:*'
                    ]
                },
            ],
        )
