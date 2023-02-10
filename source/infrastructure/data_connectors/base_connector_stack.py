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

from constructs import Construct
from aws_cdk import CfnParameter, Aws, RemovalPolicy
from aws_cdk import aws_kms as kms
import aws_cdk.aws_sns as sns
import aws_cdk.aws_dynamodb as dynamodb
from aws_cdk.aws_sns_subscriptions import EmailSubscription
from cdk_nag import NagSuppressions
from aws_solutions.cdk.stack import SolutionStack
from data_connectors.connector_buckets import ConnectorBuckets
from data_connectors.transform.databrew_transform import DataBrewTransform
from data_connectors.automatic_databrew_job_launch import AutomaticDatabrewJobLaunch
from data_connectors.orchestration.stepfunctions.base import WorkflowOrchestrator


class BaseConnectorStack(SolutionStack):
    """
    This class represents the base connectors stack
    """

    def create_schema_provider_parameter(self):
        """
        This function creates the schema provider parameter
        """
        parameter = CfnParameter(
            self,
            "SchemaProviderName",
            description="Select the name of the schema provider",
            allowed_values=["Not listed"],
            default="Not listed",
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="Select the name of the schema provider",
            group="Data",
        )
        return parameter

    def create_transform_resource(self):
        """
        This function creates the transformation construct
        """
        return DataBrewTransform(
            self,
            schema_provider_parameter=self.schema_provider_parameter
            # pass other parameters
        )

    def create_s3_push_trigger_resource(self):
        """
        This function creates the automatic databrew job launch construct
        """
        AutomaticDatabrewJobLaunch(
            self,
            schema_provider_parameter=self.schema_provider_parameter
        )

    def sns_notification_object(self):
        """
        This function creates the SNS topic for email notifications
        """
        email_param = CfnParameter(
            self,
            id="NotificationEmail",
            type="String",
            description="Email to notify with results E.g. alice@example.com",
            default="",
            max_length=50,
            allowed_pattern=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$|^$)",
            constraint_description="Must be a valid email address or blank",
        )
        self.solutions_template_options.add_parameter(
            email_param, "Email to send the notification to", "Notification Configuration"
        )

        key_from_alias = kms.Alias.from_alias_name(self, "SnsManagedKey", "alias/aws/sns")
        
        sns_topic = sns.Topic(self, "SnsNotifyTopic", master_key=key_from_alias)
        sns_topic.add_subscription(EmailSubscription(email_param.value_as_string))

        NagSuppressions.add_resource_suppressions(
            sns_topic,
            [
                {
                    "id": "AwsSolutions-SNS3",
                    "reason": " The SNS Topic does not require publishers to use SSL",
                },
            ],
        )
        return sns_topic

    def create_dynamodb_table(self):
        """
        This function create a dynamodb table to keep the last S3 object create event time
        for automatic Databrew job launch workflow
        """
        return dynamodb.Table(
            self,
            "DynamoDBTable",
            table_name=f"{Aws.STACK_NAME}-InboundBucketFileUploadTimeKeeper",
            partition_key=dynamodb.Attribute(name="watching_key", type=dynamodb.AttributeType.STRING),
            removal_policy=RemovalPolicy.DESTROY
        )

    def create_workflow(self):
        """
        This function is responsible for creating the appropriate
        subclass of the orchestration workflow construct
        Override from BaseConnectorStack
        """
        return WorkflowOrchestrator(
            self,
            "WorkflowOrchestrator",
            self.transform.recipe_name,
            self.connector_buckets.inbound_bucket.bucket_name,
            self.transform.dataset_name,
            self.transform.transform_recipe_file_location_parameter.value_as_string,
            self.transform.recipe_job_name,
            self.sns_topic,
            self.dynamodb_table,
            self.transform.recipe_lambda_custom_resource,
        )

    def __init__(self, scope: Construct, construct_id: str, *args, **kwargs) -> None:
        super().__init__(scope, construct_id, *args, **kwargs)

        #
        # stack parameters provided by this class
        #

        self.schema_provider_parameter = self.create_schema_provider_parameter()
        self.sns_topic = self.sns_notification_object()
        self.dynamodb_table = self.create_dynamodb_table()
        #
        # constructs and conditions contained by this class
        #

        # buckets and related resource for raw and transformed data
        if self.node.try_get_context("SYNTH_BUCKETS"):
            self.connector_buckets = ConnectorBuckets(self)

        if self.node.try_get_context("SYNTH_TRANSFORMS"):
            self.transform = self.create_transform_resource()

        # create the orchestration workflow
        if self.node.try_get_context("SYNTH_ORCHESTRATION"):
            # create the orchestration workflow
            self.workflow = self.create_workflow()
            # create resource for automatic brew job launch workflow
            self.create_s3_push_trigger_resource()
