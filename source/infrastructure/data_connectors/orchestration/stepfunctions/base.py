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


from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
from constructs import Construct
from aws_cdk import Aws, Fn
from aws_cdk.aws_logs import LogGroup
from cdk_nag import NagSuppressions
from data_connectors.orchestration.async_callback_construct import AsyncCallbackConstruct


class WorkflowOrchestrator(Construct):
    """The State Machine automatically launches the Databrew job to process the data stored in the s3 bucket"""

    def __init__(
            self,
            scope: Construct,
            id: str,
            recipe_name,
            s3_bucket_name,
            dataset_name,
            recipe_bucket,
            recipe_job_name,
            sns_topic,
            dynamodb_table,
            recipe_lambda_custom_resource,
    ):
        """
        Create a new ingestion workflow
        """

        self.recipe_name = recipe_name
        self.s3_bucket_name = s3_bucket_name
        self.dataset_name = dataset_name
        self.recipe_bucket = recipe_bucket
        self.recipe_job_name = recipe_job_name
        self.sns_topic = sns_topic
        self.dynamodb_table = dynamodb_table
        self.recipe_lambda_custom_resource = recipe_lambda_custom_resource

        self.base_state_machine_name = f"{Aws.STACK_NAME}-S3TriggerDatabrewJob-Runner"

        super().__init__(scope, id)

        self.async_callback_construct = AsyncCallbackConstruct(
            self, "WorkflowOrchestration", self.recipe_job_name, self.base_state_machine_name,
        )

        self.state_machine = self.create_base_workflow()

        # Prevent workflow is triggered by the sample file in the inbound bucket on create
        self.state_machine.node.add_dependency(self.recipe_lambda_custom_resource)
        self.state_machine.node.add_dependency(self.async_callback_construct)

        self.cdk_nag_suppression()

    def create_base_workflow(self):
        log_group_name = f"/aws/vendedlogs/states/{Aws.STACK_NAME}-{Fn.select(2, Fn.split('/', Aws.STACK_ID))}"

        return sfn.StateMachine(
            self,
            "S3TriggerDatabrewRunner",
            tracing_enabled=True,
            state_machine_name=self.base_state_machine_name,
            definition=self.chain,
            logs=sfn.LogOptions(level=sfn.LogLevel.ALL,
                                destination=LogGroup(self, 'SFNLogGroup', log_group_name=log_group_name))
        )

    @property
    def chain(self) -> sfn.Chain:
        """
        Get the Chain of steps that will accommodate countdown timer to check s3 object
        create status and launch the brew job.
        :return: the Chain of steps
        """
        wait = sfn.Wait(self, "Wait", time=sfn.WaitTime.seconds_path("$.waiting_time_in_seconds"))

        dynamodb_get_item = tasks.DynamoGetItem(self, "DynamoDB Get Last File Uploaded Time",
                                                key={"watching_key": tasks.DynamoAttributeValue.from_string(
                                                    sfn.JsonPath.string_at("$.watching_key"))},
                                                table=self.dynamodb_table,
                                                result_path="$.dynamodb_response",
                                                consistent_read=True)

        file_uploading_pass = sfn.Pass(self, "File Uploading")

        trigger_data_transform_workflow = self.invoke_lambda_run_brew_jobs().next(self.publish_notification())

        choice = sfn.Choice(self, "Check File Upload Status")
        choice.when(sfn.Condition.timestamp_less_than_equals_json_path("$.dynamodb_response.Item.timestamp_str.S",
                                                                       "$.expected_upload_finish_time_str"),
                    trigger_data_transform_workflow)
        choice.otherwise(file_uploading_pass)

        state_machine_definition = wait.next(dynamodb_get_item).next(choice)

        return state_machine_definition

    def publish_notification(self):
        """
        Function to run the tasks to publish the outcome of the step function
        """
        return tasks.SnsPublish(self, "Databrew Job Notification",
            topic=self.sns_topic,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            message=sfn.TaskInput.from_text("Databrew Job is Done and Orchestration Completed."),
            subject=sfn.JsonPath.format("Data Connectors for AWS Clean Rooms Notifications: Pipeline result [{}]", sfn.JsonPath.string_at("$.status"))
        )

    def invoke_lambda_run_brew_jobs(self):
        """
        Function to invoke the brew job lambda and run it subsequently
        """

        return tasks.LambdaInvoke(
            self, 'Launch Databrew Job',
            lambda_function=self.async_callback_construct.brew_run_job_lambda,
            integration_pattern=sfn.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
            payload=sfn.TaskInput.from_object(
                {
                    "task_token": sfn.JsonPath.string_at("$$.Task.Token"),
                    "brew_job_name": self.recipe_job_name
                }
            )
        )

    def cdk_nag_suppression(self):
        nag_suppresion_reason = "The IAM entity contains wildcard permissions"
        NagSuppressions.add_resource_suppressions(
            self.state_machine.role.node.try_find_child("DefaultPolicy").node.find_child("Resource"),
            [
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ['Resource::*']
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ['Resource::<WorkflowOrchestrationAsyncCallbackConstructBrewRunJob52812FF2.Arn>:*']
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ["Resource::<HeadlessTransform07473135.Arn>:*"]
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ["Resource::<WorkflowOrchestrationBrewRunJob463FED05.Arn>:*"]
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ["Resource::<ConnectorUpdateFunction80A21979.Arn>:*"]
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ["Resource::<SalesforceWorkflowOrchestrationBrewRunJobC2FF95FB.Arn>:*"]
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ["Resource::<SalesforceWorkflowWorkflowOrchestrationBrewRunJob55408E3A.Arn>:*"]
                },
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": nag_suppresion_reason,
                    "appliesTo": ["Resource::<WorkflowOrchestratorWorkflowOrchestrationBrewRunJob4557B9A3.Arn>:*"]
                },
            ],
        )
