# pylint: disable=line-too-long,too-many-instance-attributes
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
from aws_cdk import CfnParameter, Duration, CustomResource, Fn, SecretValue, CfnOutput, Aws
from aws_cdk import aws_iam, aws_appflow, aws_lambda, aws_secretsmanager

from cdk_nag import NagSuppressions

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from data_connectors.automatic_databrew_job_launch import AutomaticDatabrewJobLaunch

from data_connectors.aws_lambda import LAMBDA_PATH
from data_connectors.aws_lambda.layers.aws_solutions.layer import SolutionsLayer

from data_connectors.appflow_pull_stack import AppFlowPullStack
from data_connectors.orchestration.stepfunctions.workflows.salesforce_workflow import SalesforceWorkflow


class SalesforceMarketingCloudStack(AppFlowPullStack):
    """
    This class represents the base connectors stack
    """

    name = "SalesforceMarketingCloudStack"
    description = "Deploy and use a connector for Salesforce Marketing Cloud data"
    template_filename = "salesforce-marketing-cloud-connector.template"

    connector_profile = LAMBDA_PATH / "connectors" / "salesforce" / "connector_profile.py"

    def create_client_id_parameter(self):
        """
        This function creates the client_id parameter
        """
        parameter = CfnParameter(
            self,
            "ClientId",
            description="The Client Id of the API Integration",
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="The Client Id of the API Integration",
            group="Connection",
        )
        return parameter

    def create_client_secret_parameter(self):
        """
        This function creates the client_secret parameter
        """
        parameter = CfnParameter(
            self,
            "ClientSecret",
            description="The Client Secret of the API Integration",
            no_echo=True,
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="The Client Secret of the API Integration",
            group="Connection",
        )
        return parameter

    def create_authentication_base_uri_parameter(self):
        """
        This function creates the authentication_base_uri parameter
        """
        parameter = CfnParameter(
            self,
            "AuthenticationBaseURI",
            description="The Authentication Base URI of the API Integration",
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="The Authentication Base URI of the API Integration",
            group="Connection",
        )
        return parameter

    def create_rest_base_uri_parameter(self):
        """
        This function creates the rest_base_uri parameter
        """
        parameter = CfnParameter(
            self,
            "RESTBaseURI",
            description="The REST Base URI of the API Integration",
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="The REST Base URI of the API Integration",
            group="Connection",
        )
        return parameter

    def create_schema_provider_parameter(self):
        """
        This function creates the schema provider parameter
        This is an override from BaseConnectorStack
        """
        parameter = CfnParameter(
            self,
            "SchemaProviderName",
            allowed_values=["Salesforce Marketing Cloud"],
            default="Salesforce Marketing Cloud",
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="Select a schema provider",
            group="Data",
        )
        return parameter

    def create_salesforce_object_parameter(self):
        """
        This function creates the data object parameter used to specific what data to pull from SFMC
        """
        parameter = CfnParameter(
            self,
            "SalesforceObjectName",
            allowed_values=[
                "Activity",
                "BounceEvent",
                "ClickEvent",
                "ContentArea",
                "DataExtension",
                "Email",
                "ForwardedEmailEvent",
                "ForwardedEmailOptInEvent",
                "Link",
                "LinkSend",
                "List",
                "ListSubscriber",
                "NotSentEvent",
                "OpenEvent",
                "Send",
                "SentEvent",
                "Subscriber",
                "SurveyEvent",
                "UnsubEvent",
                "AuditEvents",
                "Campaigns",
                "Interactions",
                "ContentAssets",
            ],
            default="ClickEvent",
        )
        self.solutions_template_options.add_parameter(
            parameter,
            label="Select a Salesforce data object to pull",
            group="Data",
        )
        return parameter

    def update_inbound_bucket_policy(self):
        """
        This function is responsible for updating the inbound data bucket policy
        with permission for AppFlow to read/write the bucket
        """
        dest_bucket = self.connector_buckets.inbound_bucket
        dest_bucket_policy = aws_iam.PolicyStatement(
            principals=[aws_iam.ServicePrincipal("appflow.amazonaws.com")],
            actions=[
                "s3:putobject",
                "s3:getbucketacl",
                "s3:putobjectacl",
                "s3:abortmultipartupload",
                "s3:listmultipartuploadparts",
                "s3:listbucketmultipartuploads",
            ],
            resources=[
                f"{dest_bucket.bucket_arn}", f"{dest_bucket.bucket_arn}/*"
            ],
        )
        dest_bucket.add_to_resource_policy(dest_bucket_policy)

    def create_lambda_appflow_policies(self) -> list[aws_iam.PolicyStatement]:
        """
        This function is responsible for defining a policy to
        allow AppFlow connection and flow CRUD requests
        """
        dest_bucket = self.connector_buckets.inbound_bucket
        return [
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "appflow:CreateConnectorProfile",
                    "appflow:List*",
                    "appflow:Describe*",
                    "kms:List*",
                    "kms:Describe*",
                ],
                resources=["*"],
            ),
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "appflow:UpdateConnectorProfile",
                    "appflow:DeleteConnectorProfile",
                ],
                resources=[
                    f"arn:{Aws.PARTITION}:appflow:{Aws.REGION}:{Aws.ACCOUNT_ID}:connectorprofile/{self.profile_name}"
                ],
            ),
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "secretsmanager:CreateSecret",
                    "secretsManager:PutResourcePolicy",
                    "secretsmanager:PutSecretValue",
                    "secretsmanager:GetSecretValue",
                ],
                resources=[
                    f"arn:{Aws.PARTITION}:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:appflow!*",
                    f"arn:{Aws.PARTITION}:secretsmanager:{Aws.REGION}:{Aws.ACCOUNT_ID}:secret:AppFlowConnectionSecret*",
                ],
            ),
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                    "s3:putobject",
                    "s3:getbucketacl",
                    "s3:putobjectacl",
                    "s3:abortmultipartupload",
                    "s3:listmultipartuploadparts",
                    "s3:listbucketmultipartuploads",
                ],
                resources=[
                    f"{dest_bucket.bucket_arn}",
                    f"{dest_bucket.bucket_arn}/*",
                ],
            ),
        ]

    def create_connector_custom_resource_function(self):
        """
        This function is responsible for creating the Python function resource
        used by the custom resource to create/update/delete the AppFlow connection profile
        via CloudFormation
        """
        connector_custom_resource_function = SolutionsPythonFunction(
            self,
            "ConnectorCustomResourceFunction",
            LAMBDA_PATH / "custom_resource" / "salesforce" / "connector_profile.py",
            "event_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            description=
            "Lambda function for custom resource for connector profiles",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=aws_lambda.Architecture.ARM_64,
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ],
        )
        connector_custom_resource_function.add_environment(
            "SOLUTION_ID", self.solution_id)
        connector_custom_resource_function.add_environment(
            "SOLUTION_VERSION", self.solution_version)
        return connector_custom_resource_function

    def create_connector_create_function(self):
        """
        This function is responsible for creating the Python function resource
        used by the orchestration workflow for updating the connector's access token
        """
        connector_function = SolutionsPythonFunction(
            self,
            "ConnectorCreateFunction",
            self.connector_profile,
            "create_event_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            description="Lambda function for connector profile create",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=aws_lambda.Architecture.ARM_64,
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ],
        )
        connector_function.add_environment("SOLUTION_ID", self.solution_id)
        connector_function.add_environment("SOLUTION_VERSION",
                                           self.solution_version)
        connector_function.add_environment(
            "CONNECTOR_SECRET_ARN", self.appflow_connection_secret.secret_arn)
        return connector_function

    def create_connector_update_function(self):
        """
        This function is responsible for creating the Python function resource
        used by the orchestration workflow for updating the connector's access token
        """
        connector_function = SolutionsPythonFunction(
            self,
            "ConnectorUpdateFunction",
            self.connector_profile,
            "update_event_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            description="Lambda function for connector profile update",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=aws_lambda.Architecture.ARM_64,
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ],
        )
        connector_function.add_environment("SOLUTION_ID", self.solution_id)
        connector_function.add_environment("SOLUTION_VERSION",
                                           self.solution_version)
        connector_function.add_environment(
            "CONNECTOR_SECRET_ARN", self.appflow_connection_secret.secret_arn)
        return connector_function

    def create_connector_delete_function(self):
        """
        This function is responsible for creating the Python function resource
        used by the orchestration workflow for updating the connector's access token
        """
        connector_function = SolutionsPythonFunction(
            self,
            "ConnectorDeleteFunction",
            self.connector_profile,
            "delete_event_handler",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            description=
            "Lambda function for connector profile create, update, delete",
            timeout=Duration.minutes(5),
            memory_size=256,
            architecture=aws_lambda.Architecture.ARM_64,
            layers=[
                PowertoolsLayer.get_or_create(self),
                SolutionsLayer.get_or_create(self)
            ],
        )
        connector_function.add_environment(
            "CONNECTOR_SECRET_ARN", self.appflow_connection_secret.secret_arn)
        return connector_function

    def create_connector_profile_custom_resource(self):
        """
        This function creates the custom resource used to create new connector profiles
        """
        return CustomResource(
            self,
            "ConnectorProfileCustomResource",
            service_token=self.connector_custom_resource_function.function_arn,
            properties={
                "profile_name":
                    self.profile_name,
                "client_id":
                    self.client_id_parameter.value_as_string,
                "client_secret":
                    self.client_secret_parameter.value_as_string,
                "token_endpoint":
                    Fn.sub(
                        "${base}/v2/token",
                        {
                            "base":
                                self.authentication_base_uri_parameter.value_as_string
                        },
                    ),
                "instance_url":
                    self.rest_base_uri_parameter.value_as_string,
            },
        )

    def create_appflow_resource(self):
        """
        This function is responsible for creating the AppFlow flow
        resource with an SFMC source and S3 destination
        """
        appflow_resource = aws_appflow.CfnFlow(
            self,
            "SalesforceAppFlow",
            destination_flow_config_list=[
                aws_appflow.CfnFlow.DestinationFlowConfigProperty(
                    connector_type="S3",
                    destination_connector_properties=aws_appflow.CfnFlow.
                    DestinationConnectorPropertiesProperty(
                        s3=aws_appflow.CfnFlow.S3DestinationPropertiesProperty(
                            bucket_name=self.connector_buckets.inbound_bucket.
                            bucket_name,
                            s3_output_format_config=aws_appflow.CfnFlow.
                            S3OutputFormatConfigProperty(
                                aggregation_config=aws_appflow.CfnFlow.
                                AggregationConfigProperty(
                                    aggregation_type="SingleFile"),
                                file_type="JSON",
                                preserve_source_data_typing=False,
                            ),
                        )),
                )
            ],
            flow_name=f"{Aws.STACK_NAME}-flow",
            source_flow_config=aws_appflow.CfnFlow.SourceFlowConfigProperty(
                connector_profile_name=self.profile_name,
                connector_type="CustomConnector",
                api_version="v1",
                source_connector_properties=aws_appflow.CfnFlow.
                SourceConnectorPropertiesProperty(
                    custom_connector=aws_appflow.CfnFlow.
                    CustomConnectorSourcePropertiesProperty(
                        entity_name=self.salesforce_object_parameter.
                        value_as_string,
                        custom_properties={},
                    )),
            ),
            tasks=[
                aws_appflow.CfnFlow.TaskProperty(source_fields=[],
                                                 task_type="Map_all")
            ],
            trigger_config=aws_appflow.CfnFlow.TriggerConfigProperty(
                trigger_type="OnDemand"),
            # the properties below are optional
            description="Salesforce Marketing Cloud to S3 flow",
        )
        appflow_resource.node.add_dependency(
            self.connector_profile_custom_resource)
        return appflow_resource

    def create_appflow_connection_secret(self):
        """
        This function is responsible for creating the secret containing the AppFlow connection secrets
        """
        return aws_secretsmanager.Secret(
            self,
            "AppFlowConnectionSecret",
            secret_string_value=SecretValue.unsafe_plain_text(
                Fn.to_json_string({
                    "profile_name":
                        self.profile_name,
                    "client_id":
                        self.client_id_parameter.value_as_string,
                    "client_secret":
                        self.client_secret_parameter.value_as_string,
                    "token_endpoint":
                        Fn.sub(
                            "${base}/v2/token",
                            {
                                "base":
                                    self.authentication_base_uri_parameter.
                                    value_as_string
                            },
                        ),
                    "instance_url":
                        self.rest_base_uri_parameter.value_as_string,
                })),
        )

    def add_cdk_nag_suppressions(self):
        nag_suppression_reason_for_wildcard_permissions = "The IAM entity contains wildcard permissions"
        for path in [
            "/SalesforceMarketingCloudStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource",
        ]:
            NagSuppressions.add_resource_suppressions_by_path(
                self,
                path,
                [
                    {
                        "id": "AwsSolutions-IAM4",
                        "reason": "The IAM user, role, or group uses AWS managed policies",
                    },
                ],
            )

        # lambda functions
        for path in [
            "/SalesforceMarketingCloudStack/ConnectorCustomResourceFunction-Role/Resource",
            "/SalesforceMarketingCloudStack/ConnectorCreateFunction-Role/Resource",
            "/SalesforceMarketingCloudStack/ConnectorUpdateFunction-Role/Resource",
            "/SalesforceMarketingCloudStack/ConnectorDeleteFunction-Role/Resource",
        ]:
            NagSuppressions.add_resource_suppressions_by_path(
                self,
                path,
                [
                    {
                        "id": "AwsSolutions-IAM5",
                        "reason": nag_suppression_reason_for_wildcard_permissions,
                    },
                ],
            )
        for path in [
            "/SalesforceMarketingCloudStack/ConnectorCustomResourceFunction-Role/DefaultPolicy/Resource",
            "/SalesforceMarketingCloudStack/ConnectorCreateFunction-Role/DefaultPolicy/Resource",
            "/SalesforceMarketingCloudStack/ConnectorUpdateFunction-Role/DefaultPolicy/Resource",
            "/SalesforceMarketingCloudStack/ConnectorDeleteFunction-Role/DefaultPolicy/Resource",
            "/SalesforceMarketingCloudStack/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/DefaultPolicy/Resource",
        ]:
            NagSuppressions.add_resource_suppressions_by_path(
                self,
                path,
                [
                    {
                        "id": "AwsSolutions-IAM5",
                        "reason": nag_suppression_reason_for_wildcard_permissions,
                    },
                ],
            )

        # secrets manager
        NagSuppressions.add_resource_suppressions_by_path(
            self,
            "/SalesforceMarketingCloudStack/AppFlowConnectionSecret/Resource",
            [
                {
                    "id": "AwsSolutions-SMG4",
                    "reason": "This secret's content does not support an auto-rotation process",
                },
            ],
        )

        # Sqs
        NagSuppressions.add_resource_suppressions_by_path(
            self,
            "/SalesforceMarketingCloudStack/SqsBatching/Resource",
            [
                {
                    "id": "AwsSolutions-SQS2",
                    "reason": "The SQS Queue does not have server-side encryption enabled."
                },
                {
                    "id": "AwsSolutions-SQS3",
                    "reason": "The SQS queue does not have a dead-letter queue (DLQ) enabled or have a cdk-nag rule suppression indicating it is a DLQ."
                },
                {
                    "id": "AwsSolutions-SQS4",
                    "reason": "The SQS queue does not require requests to use SSL."
                },
            ]
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            "/SalesforceMarketingCloudStack/ProcessS3NotificationsLambdaIamPolicy/Resource",
            [
                {
                    "id": "AwsSolutions-IAM5",
                    "reason": nag_suppression_reason_for_wildcard_permissions,
                    "appliesTo": [
                        "Resource::arn:aws:states:*:<AWS::AccountId>:activity:<SalesforceWorkflowS3TriggerDataBrewRunner632E7DE9.Name>:*",
                        "Resource::arn:aws:states:*:<AWS::AccountId>:stateMachine:<SalesforceWorkflowS3TriggerDataBrewRunner632E7DE9.Name>",
                        "Resource::arn:aws:states:*:<AWS::AccountId>:execution:<SalesforceWorkflowS3TriggerDataBrewRunner632E7DE9.Name>:*"
                    ]
                }
            ],
        )

    def create_stack_outputs(self):
        CfnOutput(self,
                  "AppFlow",
                  value=f"https://{Aws.REGION}.console.aws.amazon.com/appflow/home?region={Aws.REGION}#/details/{self.appflow_flow.flow_name}",
                  )
        CfnOutput(
            self,
            "AppFlowLaunch-StateMachine",
            value=f"https://{Aws.REGION}.console.aws.amazon.com/states/home?region={Aws.REGION}#/statemachines/view/arn:aws:states:{Aws.REGION}:{Aws.ACCOUNT_ID}:stateMachine:{self.appflow_launch_state_machine_name}")

    def create_workflow(self):
        """
        This method is overridden and empty to prevent creation
        of the workflow before other dependencies are available
        """
        pass

    def create_s3_push_trigger_resource(self):
        """
        This method is overridden and empty to prevent creation
        of the resource before other dependencies are available
        """
        pass

    def create_s3_push_trigger_resource_deferred(self):
        AutomaticDatabrewJobLaunch(
            self,
            schema_provider_parameter=self.schema_provider_parameter
        )

    def create_workflow_deferred(self):
        self.appflow_launch_state_machine_name = f"{Aws.STACK_NAME}-AppflowLaunch"
        return SalesforceWorkflow(
            self, "SalesforceWorkflow",
            self.connector_update_function,
            self.appflow_flow,
            self.appflow_launch_state_machine_name,
            self.transform.recipe_name,
            self.connector_buckets.inbound_bucket.bucket_name,
            self.transform.dataset_name, self.transform.
            transform_recipe_file_location_parameter.value_as_string,
            self.transform.recipe_job_name,
            self.sns_topic,
            self.dynamodb_table,
            self.transform.recipe_lambda_custom_resource_function,
        )

    def __init__(self, scope: Construct, construct_id: str, *args,
                 **kwargs) -> None:
        # parent constructor
        super().__init__(scope, construct_id, *args, **kwargs)
        self.synthesizer.bind(self)

        # local parameters
        self.salesforce_object_parameter = self.create_salesforce_object_parameter(
        )
        self.client_id_parameter = self.create_client_id_parameter()
        self.client_secret_parameter = self.create_client_secret_parameter()
        self.authentication_base_uri_parameter = (
            self.create_authentication_base_uri_parameter())
        self.rest_base_uri_parameter = self.create_rest_base_uri_parameter()
        self.profile_name = f"{Aws.STACK_NAME}-connector"

        # update destination bucket policy for appflow
        self.update_inbound_bucket_policy()

        # create the secret for connector profile updates later
        self.appflow_connection_secret = self.create_appflow_connection_secret(
        )

        # create lambda functions used by orchestration
        self.connector_create_function = self.create_connector_create_function(
        )
        self.connector_update_function = self.create_connector_update_function(
        )
        self.connector_delete_function = self.create_connector_delete_function(
        )

        self.connector_custom_resource_function = (
            self.create_connector_custom_resource_function())

        self.connector_profile_custom_resource = (
            self.create_connector_profile_custom_resource())

        # update permissions for lambdas to call appflow and secretsmanager
        self.lambda_appflow_policies = self.create_lambda_appflow_policies()
        for policy in self.lambda_appflow_policies:
            self.connector_custom_resource_function.add_to_role_policy(policy)
            self.connector_create_function.add_to_role_policy(policy)
            self.connector_update_function.add_to_role_policy(policy)
            self.connector_delete_function.add_to_role_policy(policy)

        self.appflow_flow = self.create_appflow_resource()

        if self.node.try_get_context("SYNTH_ORCHESTRATION"):
            self.workflow = self.create_workflow_deferred()
            self.create_s3_push_trigger_resource_deferred()

        self.add_cdk_nag_suppressions()

        self.create_stack_outputs()
