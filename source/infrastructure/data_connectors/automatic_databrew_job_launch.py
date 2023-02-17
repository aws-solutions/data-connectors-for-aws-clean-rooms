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

from dataclasses import dataclass
from aws_cdk import (
    Duration,
    CfnParameter,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_lambda as lambda_,
    aws_sqs as sqs,
    aws_iam as iam,
    Aws,
)
from cdk_nag import NagSuppressions
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from data_connectors.aws_lambda.layers.aws_solutions.layer import SolutionsLayer
from data_connectors.aws_lambda import LAMBDA_PATH


class AutomaticDatabrewJobLaunch:
    def __init__(self, stack, schema_provider_parameter=None) -> None:
        stack_account_id: str = Aws.ACCOUNT_ID

        self.create_template_parameters(stack)

        self.create_lambda_iam_policy(
            stack, stack_account_id,
            stack.dynamodb_table.table_name,
            stack.workflow.state_machine.state_machine_name
        )

        self.create_s3_notification_lambda_function(stack, self.automatic_brew_job_launch,
                                                    self.file_upload_complete_waiting_time_in_minutes)

        self.create_s3_notifications_queue(stack)

        self.add_s3_notifications_to_sqs(stack)
        self.add_lambda_event_source_sqs()

    def create_template_parameters(self, stack):
        allowed_values = ["OFF", "ON"]
        group_name = "Transform"
        self.automatic_brew_job_launch = CfnParameter(
            stack,
            "AutotriggerTransform",
            description="Automatically launch databrew transform job",
            allowed_values=allowed_values,
            default=allowed_values[0]
        )
        stack.solutions_template_options.add_parameter(
            self.automatic_brew_job_launch,
            label="Automatic transform trigger",
            group=group_name,
        )

        self.file_upload_complete_waiting_time_in_minutes = CfnParameter(
            stack,
            "FileUploadCompleteWaitingTime",
            description="Inbound bucket file upload complete waiting time in minutes",
            default=1,
            min_value=0.5,
            type='Number'
        )
        stack.solutions_template_options.add_parameter(
            self.file_upload_complete_waiting_time_in_minutes,
            label="File upload complete waiting time in minutes",
            group=group_name
        )

    def create_lambda_iam_policy(self, stack, stack_account, dynamodb_table_name, state_machine_name):
        policy_statements: list[iam.PolicyStatement] = self.create_policy_statements_for_lambda(
            stack_account,
            dynamodb_table_name,
            state_machine_name)

        self.lambda_iam_policy = iam.Policy(stack, "ProcessS3NotificationsLambdaIamPolicy",
                                            statements=policy_statements)
        self.lambda_iam_policy.node.add_dependency(stack.workflow.state_machine)

        self.lambda_iam_policy_cdk_nag_suppresions(self.lambda_iam_policy)

    def create_s3_notification_lambda_function(self, stack, automatic_brew_job_launch,
                                               file_upload_complete_waiting_time_in_minutes):
        self.lambda_process_s3_notification = SolutionsPythonFunction(
            stack,
            "ProcessS3ObjectCreateNotificationFunction",
            LAMBDA_PATH / "automatic_brew_job_launch" / "lambda_function.py",
            "event_handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            description="Lambda function for processing s3 object create notification",
            timeout=Duration.minutes(1),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            layers=[PowertoolsLayer.get_or_create(stack),
                    SolutionsLayer.get_or_create(stack)],
        )
        self.lambda_process_s3_notification.add_environment("SOLUTION_ID", stack.node.try_get_context("SOLUTION_ID"))
        self.lambda_process_s3_notification.add_environment("SOLUTION_VERSION",
                                                            stack.node.try_get_context("SOLUTION_VERSION"))
        self.lambda_process_s3_notification.add_environment("DDB_TABLE_NAME", stack.dynamodb_table.table_name)
        self.lambda_process_s3_notification.add_environment("STATE_MACHINE_ARN",
                                                            stack.workflow.state_machine.state_machine_arn)
        self.lambda_process_s3_notification.add_environment("AUTOMATIC_DATABREW_JOB_LAUNCH",
                                                            automatic_brew_job_launch.value_as_string)
        self.lambda_process_s3_notification.add_environment("WAITING_TIME_IN_MINUTES",
                                                            file_upload_complete_waiting_time_in_minutes.value_as_string
                                                            )

        self.lambda_iam_policy.attach_to_role(self.lambda_process_s3_notification.role)

        self.lambda_process_s3_notification.node.add_dependency(stack.dynamodb_table)
        self.lambda_process_s3_notification.node.add_dependency(stack.workflow.state_machine)

        NagSuppressions.add_resource_suppressions(
            self.lambda_process_s3_notification.role.node.default_child,
            [
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": '* Resource needed by logs',
                    "appliesTo": [
                        "Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/lambda/*"]
                },
            ]
        )

    def create_s3_notifications_queue(self, stack):
        self.s3_notifications_queue = sqs.Queue(
            stack, "SqsBatching",
            visibility_timeout=Duration.seconds(SQSQueueParameters.visibility_timeout_in_seconds),
            queue_name=f"{Aws.STACK_NAME}-s3-notifications",
        )
        self.s3_notifications_queue.node.add_dependency(self.lambda_process_s3_notification)
        self.s3_notifications_queue.node.add_dependency(stack.connector_buckets.inbound_bucket)

    def add_s3_notifications_to_sqs(self, stack):
        stack.connector_buckets.inbound_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(self.s3_notifications_queue),
            s3.NotificationKeyFilter(
                prefix=stack.connector_buckets.inbound_bucket_prefix
            )
        )

    def add_lambda_event_source_sqs(self):
        self.lambda_process_s3_notification.add_event_source(
            SqsEventSource(
                self.s3_notifications_queue,
                batch_size=SQSQueueParameters.batch_size,
                max_batching_window=Duration.seconds(SQSQueueParameters.max_batching_window_in_seconds)
            )
        )

    def create_policy_statements_for_lambda(self, stack_account, dynamodb_table_name, state_machine_name):
        dynamodb_table_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:UpdateItem",
                "dynamodb:GetItem",
            ],
            resources=[
                f"arn:aws:dynamodb:*:{stack_account}:table/{dynamodb_table_name}/stream/*",
                f"arn:aws:dynamodb:*:{stack_account}:table/{dynamodb_table_name}",
                f"arn:aws:dynamodb:*:{stack_account}:table/{dynamodb_table_name}/index/*"
            ]
        )

        stepfunctions_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "states:StartSyncExecution",
                "states:StartExecution",
                "states:StopExecution",
                "states:ListExecutions",
            ],
            resources=[
                f"arn:aws:states:*:{stack_account}:activity:{state_machine_name}:*",
                f"arn:aws:states:*:{stack_account}:stateMachine:{state_machine_name}",
                f"arn:aws:states:*:{stack_account}:execution:{state_machine_name}:*"
            ]
        )

        policy_statements = [dynamodb_table_statement, stepfunctions_statement]
        return policy_statements

    def lambda_iam_policy_cdk_nag_suppresions(self, lambda_iam_policy):
        NagSuppressions.add_resource_suppressions(
            lambda_iam_policy,
            [
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": '* Resource applied to specific resource',
                    "appliesTo": [
                        "Resource::arn:aws:dynamodb:*:<AWS::AccountId>:table/<DynamoDBTable59784FC0>/stream/*",
                        "Resource::arn:aws:dynamodb:*:<AWS::AccountId>:table/<DynamoDBTable59784FC0>",
                        "Resource::arn:aws:dynamodb:*:<AWS::AccountId>:table/<DynamoDBTable59784FC0>/index/*",
                        "Resource::arn:aws:states:*:<AWS::AccountId>:activity:<WorkflowOrchestratorS3TriggerDataBrewRunner98B33198.Name>:*",
                        "Resource::arn:aws:states:*:<AWS::AccountId>:stateMachine:<WorkflowOrchestratorS3TriggerDataBrewRunner98B33198.Name>",
                        "Resource::arn:aws:states:*:<AWS::AccountId>:execution:<WorkflowOrchestratorS3TriggerDataBrewRunner98B33198.Name>:*",
                    ]
                },
            ],
        )


@dataclass
class SQSQueueParameters:
    visibility_timeout_in_seconds: int = 300
    batch_size: int = 30
    max_batching_window_in_seconds: int = 10
