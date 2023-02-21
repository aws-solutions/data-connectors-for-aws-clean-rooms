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

import aws_cdk.aws_dynamodb as dynamodb
import aws_cdk.aws_events as events
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as lambda_
from aws_cdk import Duration, RemovalPolicy, Aws
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions_constructs.aws_eventbridge_lambda import EventbridgeToLambda
from aws_solutions_constructs.aws_lambda_dynamodb import LambdaToDynamoDB
from cdk_nag import NagSuppressions
from constructs import Construct
from data_connectors.aws_lambda import LAMBDA_PATH
from data_connectors.aws_lambda.layers.aws_solutions.layer import SolutionsLayer


class AsyncCallbackConstruct(Construct):

    def __init__(
            self,
            scope: Construct,
            id: str,
            job_name: str,
            workflow_name: str,
            *args,
            **kwargs,
    ):

        super().__init__(scope, id)
        self.job_name = job_name
        self.workflow_name = workflow_name

        self.brew_run_job_lambda = SolutionsPythonFunction(
            self,
            "BrewRunJob",
            LAMBDA_PATH / "brew_run_job" / "lambda_function.py",
            function="handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            description="This function run the brew job and store token in DDB",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self),
            ]
        )

        self.callback_lambda_function = SolutionsPythonFunction(
            self,
            "StepFunctionsCallback",
            LAMBDA_PATH / "step_function_call_back" / "lambda_function.py",
            "handler",
            runtime=lambda_.Runtime.PYTHON_3_9,
            description="This function read dynamodb table and send token back to step function",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=lambda_.Architecture.ARM_64,
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self),
            ]
        )

        lambda_to_put_token_in_dynamo = LambdaToDynamoDB(
            self,
            "WriteTokenToDB",
            dynamo_table_props=dynamodb.TableProps(
                partition_key=dynamodb.Attribute(
                    name="job_id", type=dynamodb.AttributeType.STRING),
                time_to_live_attribute="exp_timestamp",
                removal_policy=RemovalPolicy.DESTROY),
            existing_lambda_obj=self.brew_run_job_lambda,
            table_permissions="Write")

        self.dynamo_table = lambda_to_put_token_in_dynamo.dynamo_table

        for func_ in [self.callback_lambda_function, self.brew_run_job_lambda]:
            func_.add_environment(
                "SOLUTION_ID", self.node.try_get_context("SOLUTION_ID")
            )
            func_.add_environment(
                "SOLUTION_VERSION", self.node.try_get_context("SOLUTION_VERSION")
            )

        LambdaToDynamoDB(self, "ReadTokenFromDB",
                         existing_table_obj=self.dynamo_table,
                         existing_lambda_obj=self.callback_lambda_function,
                         table_permissions="Read"
                         )

        EventbridgeToLambda(
            self,
            "TriggerLambdaAfterBrew",
            existing_lambda_obj=self.callback_lambda_function,
            event_rule_props=events.RuleProps(
                event_pattern=events.EventPattern(source=["aws.databrew"], detail_type=["DataBrew Job State Change"])
            ),
        )

        self.attach_policy_to_role()
        self.cdk_nag_suppressions()

    def attach_policy_to_role(self):
        stack_region: str = Aws.REGION
        stack_account: str = Aws.ACCOUNT_ID
        self.lambda_brew_run_policy = iam.Policy(
            self,
            "LambdaBrewRunPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:SendTaskFailure",
                        "states:SendTaskHeartbeat",
                    ],
                    resources=[
                        f"arn:aws:states:{stack_region}:{stack_account}:stateMachine:{self.workflow_name}",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "databrew:*"
                    ],
                    resources=[f"arn:aws:databrew:{stack_region}:{stack_account}:job/{self.job_name}"],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:PutItem"
                    ],
                    resources=[f"{self.dynamo_table.table_arn}"],
                )
            ],
        )

        self.lambda_callback_policy = iam.Policy(
            self,
            "LambdaCallbackPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "states:SendTaskFailure",
                        "states:SendTaskSuccess",
                    ],
                    resources=[
                        f"arn:aws:states:{stack_region}:{stack_account}:stateMachine:{self.workflow_name}",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:Query"
                    ],
                    resources=[f"{self.dynamo_table.table_arn}"],
                )
            ],
        )

        self.lambda_brew_run_policy.attach_to_role(self.brew_run_job_lambda.role)
        self.lambda_callback_policy.attach_to_role(self.callback_lambda_function.role)

    def cdk_nag_suppressions(self):

        list_of_cdk_nags_to_suppress = [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": "The IAM entity contains wildcard permissions",
                "appliesTo": [
                    'Resource::*',
                    'Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/lambda/*',

                ],

            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": 'Databrew actions with * needs to suppressed',
                "appliesTo": ['Action::databrew:*']
            }
        ]

        for func_ in [self.brew_run_job_lambda, self.callback_lambda_function,
                      self.lambda_callback_policy, self.lambda_brew_run_policy]:
            NagSuppressions.add_resource_suppressions(
                func_.role if hasattr(func_, "role") else func_,
                list_of_cdk_nags_to_suppress if hasattr(func_, "role") else list_of_cdk_nags_to_suppress[1:]
            )
