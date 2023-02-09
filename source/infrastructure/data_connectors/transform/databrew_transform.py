# pylint: disable=line-too-long
"""
This module is responsible for creating the DataBrew resources for data transformation.
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

import json
from enum import Enum
from aws_cdk import (Aws, CfnCondition, CfnOutput, CfnParameter, CfnResource,
                     CustomResource, Duration, Fn)
from aws_cdk import aws_databrew as databrew
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kms as kms
from aws_cdk import aws_lambda as lambdaf
from aws_solutions.cdk.aws_lambda.layers.aws_lambda_powertools import PowertoolsLayer
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from cdk_nag import NagSuppressions
from data_connectors.aws_lambda import LAMBDA_PATH
from data_connectors.aws_lambda.layers.aws_solutions.layer import SolutionsLayer

DATABREW_CUSTOM_SECRETS_PREFIX = "AwsGlueDataBrew-transform-secret"


class InboundDataUploadType(Enum):
    Bulk = 0
    Incremental = 1


class InboundDataFileFormat(Enum):
    JSON = 0
    JSONMultiLine = 1
    CSV = 2
    CsvWithHeaderRow = 3


class DataBrewTransform:
    def __init__(self,
        stack,
        schema_provider_parameter=None) -> None:

        inbound_bucket_name: str      =   stack.connector_buckets.inbound_bucket.bucket_name
        inbound_bucket_prefix: str    =   stack.connector_buckets.inbound_bucket_prefix
        transform_bucket_name: str    =   stack.connector_buckets.transform_bucket.bucket_name
        transform_bucket_prefix: str  =   stack.connector_buckets.transform_bucket_prefix
        inbound_bucket_arn   =   stack.connector_buckets.inbound_bucket.bucket_arn
        transform_bucket_arn =   stack.connector_buckets.transform_bucket.bucket_arn

        stack_region: str = Aws.REGION
        stack_account_id: str = Aws.ACCOUNT_ID
        stack_name: str = Aws.STACK_NAME

        create_string_lambda_resource(self, stack, input_string=stack_name)
        lower_case_name = self.string_lambda_custom_resource.get_att_string("output_string")

        set_transform_component_names(self, prefix=lower_case_name)

        create_transform_template_parameters(self, stack)

        create_recipe_lambda_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix, stack_region, stack_account_id)

        create_glue_database(self, stack)

        create_glue_table(self, stack, transform_bucket_name, transform_bucket_prefix)

        create_dataset_for_transform(self, stack, inbound_bucket_name, inbound_bucket_prefix)

        databrew_iam_role: iam.Role = create_databrew_iam_role(self, stack, inbound_bucket_arn, transform_bucket_arn, stack_region, stack_account_id)

        job_encryption_key: kms.Key = create_kms_key(stack, databrew_iam_role, stack_region)

        attach_kms_policy_to_databrew_role(stack, databrew_iam_role, stack_region, stack_account_id, job_encryption_key)

        # create profile job to be optionally run by the user
        create_profile_job_for_dataset(self, stack, transform_bucket_name, transform_bucket_prefix, databrew_iam_role.role_arn, job_encryption_key.key_arn)

        create_project_with_recipe(self, stack, databrew_iam_role.role_arn)

        create_recipe_job_ref_project(self, stack, transform_bucket_name, transform_bucket_prefix, databrew_iam_role.role_arn, job_encryption_key.key_arn)

        create_object_remove_lambda_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix)

        custom_lambda_resource_function_nag_suppression(
            self.string_lambda_custom_resource_function, 
            self.recipe_lambda_custom_resource_function, 
            self.object_remove_lambda_custom_resource_function
        )

        create_stack_outputs(self, stack)


def set_transform_component_names(self, prefix='') -> None:
    self.dataset_name       = f"{prefix}-transform-dataset"
    self.recipe_name        = f"{prefix}-transform-recipe"
    self.profile_job_name   = f"{prefix}-transform-profilejob"
    self.project_name       = f"{prefix}-transform-project"
    self.recipe_job_name    = f"{prefix}-transform-recipejob"
    self.glue_database_name = f"{prefix}-transfrom-gluedb"
    self.glue_table_name    = f"{prefix}-transfrom-gluetable"


def attach_kms_policy_to_databrew_role(stack, databrew_iam_role, stack_region, stack_account_id, encryption_key) -> None:
    output_kms_policy_statement = iam.PolicyStatement(
        actions=[
            "kms:GenerateDataKey*",
        ],
        resources=[
            f"arn:aws:kms:{stack_region}:{stack_account_id}:key/{encryption_key.key_id}",
        ],
        effect=iam.Effect.ALLOW,
    )
    output_kms_policies = iam.Policy(stack, "OutputKMSPolicy",
        statements=[output_kms_policy_statement]
    )
    databrew_iam_role.attach_inline_policy(output_kms_policies)
    NagSuppressions.add_resource_suppressions(
        output_kms_policies,
            [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": '* Actions applied to specific resource',
                "appliesTo": ["Action::kms:GenerateDataKey*"]
            },
        ]
    )


def create_kms_key(stack, databrew_iam_role, stack_region) -> kms.Key:
    kms_logs_service_principal = iam.ServicePrincipal(
        f"logs.{stack_region}.amazonaws.com"
    )

    kms_policy_statement = iam.PolicyStatement(
        sid="Allow use of the encryption key",
        actions=[
            "kms:Encrypt*",
            "kms:Decrypt*",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:Describe*"
        ],
        principals=[
            kms_logs_service_principal,
            databrew_iam_role
        ],
        resources=[
            "*"
        ]
    )

    kms_key = kms.Key(stack, "BrewCfnJobEncryptionKey",
        enable_key_rotation=True,
    )
    kms_key.add_to_resource_policy(statement=kms_policy_statement)

    return kms_key


def create_databrew_iam_role(self, stack, inbound_bucket_arn, transform_bucket_arn, stack_region, stack_account_id) -> iam.Role:
    databrew_iam_role_policy_document: iam.PolicyDocument = create_policies_for_databrew_transform(self, inbound_bucket_arn, transform_bucket_arn,
                                                                                                        stack_region, stack_account_id)
    databrew_iam_role = create_databrew_transform_iam_role(stack, databrew_iam_role_policy_document)
    databrew_role_apply_nag_suppressions(databrew_iam_role)
    
    return databrew_iam_role


def create_recipe_lambda_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix, stack_region, stack_account_id) -> None:
    policy_statements: list[iam.PolicyStatement] = create_policy_statements_for_custom_lambda(self, stack, stack_region, stack_account_id,
                                                                                                inbound_bucket_name, inbound_bucket_prefix)
    self.recipe_lambda_iam_policy = iam.Policy(stack, "RecipeLambdaIamPolicy", statements=policy_statements)
    create_recipe_lambda_custom_function(self, stack, self.recipe_lambda_iam_policy)
    create_recipe_lambda_custom_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix)
    recipe_policy_apply_nag_suppresions(self.recipe_lambda_iam_policy)



def create_string_lambda_resource(self, stack, input_string):
    create_string_lambda_custom_function(self, stack)
    create_string_lambda_custom_resource(self, stack, input_string)


def create_string_lambda_custom_function(self, stack):
    """
    This function is responsible for string manipulation to lower case
    """
    self.string_lambda_custom_resource_function = SolutionsPythonFunction(
        stack,
        "StringCustomLambdaFunction",
        LAMBDA_PATH / "custom_resource" / "string_tolower" / "string_tolower.py",
        "event_handler",
        runtime=lambdaf.Runtime.PYTHON_3_9,
        description="Lambda function for string manipulation to lower case",
        timeout=Duration.minutes(1),
        memory_size=256,
        architecture=lambdaf.Architecture.ARM_64,
        layers=[PowertoolsLayer.get_or_create(stack),
                SolutionsLayer.get_or_create(stack)],
    )
    self.string_lambda_custom_resource_function.add_environment(
        "SOLUTION_ID", stack.node.try_get_context("SOLUTION_ID")
    )
    self.string_lambda_custom_resource_function.add_environment(
        "SOLUTION_VERSION", stack.node.try_get_context("SOLUTION_VERSION")
    )


def create_string_lambda_custom_resource(self, stack, input_string):
    """
    This function creates the custom resource used for string manipulation to lower case
    """
    self.string_lambda_custom_resource = CustomResource(stack,
        "StringTolowerCustomResource",
        service_token=self.string_lambda_custom_resource_function.function_arn,
        properties={
            "input_string": input_string
        }
    )


def create_object_remove_lambda_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix) -> None:
    policy_statements: list[iam.PolicyStatement] = create_policy_statements_for_object_remove_lambda(inbound_bucket_name, inbound_bucket_prefix)
    self.object_remove_lambda_iam_policy = iam.Policy(stack, "ObjectRemoveLambdaIamPolicy", statements=policy_statements)
    object_remove_lambda_custom_function(self, stack, self.object_remove_lambda_iam_policy)
    object_remove_lambda_custom_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix)
    object_remove_policy_apply_nag_suppresions(self.object_remove_lambda_iam_policy)


def create_policy_statements_for_object_remove_lambda(inbound_bucket_name, inbound_bucket_prefix) -> list[iam.PolicyStatement]:
    databrew_inbound_bucket_prefix_statement = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=[
            "s3:ListObject",
            "s3:GetObject",
            "s3:DeleteObject"
        ],
        resources=[
            f"arn:aws:s3:::{inbound_bucket_name}/{inbound_bucket_prefix}*",
        ]
    )

    return [
        databrew_inbound_bucket_prefix_statement
    ]


def object_remove_lambda_custom_function(self, stack, object_remove_lambda_iam_policy):
    """
    This function is responsible for removing the empty object file placed in the inbound bucket 
    """
    self.object_remove_lambda_custom_resource_function = SolutionsPythonFunction(
        stack,
        "ObjectRemoveCustomLambdaFunction",
        LAMBDA_PATH / "custom_resource" / "remove_placeholder" / "remove_placeholder.py",
        "event_handler",
        runtime=lambdaf.Runtime.PYTHON_3_9,
        description="Lambda function for removing the empty object file placed in the inbound bucket ",
        timeout=Duration.minutes(1),
        memory_size=256,
        architecture=lambdaf.Architecture.ARM_64,
        layers=[PowertoolsLayer.get_or_create(stack),
                SolutionsLayer.get_or_create(stack)],
    )
    self.object_remove_lambda_custom_resource_function.add_environment(
        "SOLUTION_ID", stack.node.try_get_context("SOLUTION_ID")
    )
    self.object_remove_lambda_custom_resource_function.add_environment(
        "SOLUTION_VERSION", stack.node.try_get_context("SOLUTION_VERSION")
    )
    object_remove_lambda_iam_policy.attach_to_role(self.object_remove_lambda_custom_resource_function.role)


def object_remove_lambda_custom_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix):
    """
    This function creates the custom resource used for object_remove_lambda_custom_function
    """
    self.object_remove_lambda_custom_resource = CustomResource(stack,
        "ObjectRemoveCustomResource",
        service_token=self.object_remove_lambda_custom_resource_function.function_arn,
         properties={
            "inbound_bucket_name": inbound_bucket_name,
            "inbound_bucket_prefix": inbound_bucket_prefix
        },
    )
    self.object_remove_lambda_custom_resource.node.add_dependency(self.object_remove_lambda_iam_policy)
    self.object_remove_lambda_custom_resource.node.add_dependency(self.recipe_lambda_custom_resource)
    self.object_remove_lambda_custom_resource.node.add_dependency(self.cfn_project)
    self.object_remove_lambda_custom_resource.node.add_dependency(self.cfn_job_profile_type)


def create_transform_template_parameters(self, stack) -> None:
    group_name = "Transform"
    self.transform_recipe_file_location_parameter = CfnParameter(stack,
        "TransformRecipeFileLocation",
        description="S3 location of recipe file (json) E.g. s3_folder/recipe.json",
        default="",
    )
    stack.solutions_template_options.add_parameter(
        self.transform_recipe_file_location_parameter,
        label="Location of the recipe file in S3 - Optional",
        group=group_name,
    )

    stack_specific_allowed_values = [e.name for e in InboundDataUploadType]
    if f"{stack.stack_name}" == "SalesforceMarketingCloudStack":
        stack_specific_allowed_values = [InboundDataUploadType.Bulk.name]
    elif f"{stack.stack_name}" == "S3PushStack":
        stack_specific_allowed_values = [
            InboundDataUploadType.Bulk.name,
            InboundDataUploadType.Incremental.name
        ]

    self.transform_inbound_datafile_type_parameter = CfnParameter(stack,
        "InboundDataUploadType",
        description="Data upload type",
        allowed_values=stack_specific_allowed_values,
        default=stack_specific_allowed_values[0]
    )
    stack.solutions_template_options.add_parameter(
        self.transform_inbound_datafile_type_parameter,
        label="Inbound data upload type",
        group=group_name,
    )

    stack_specific_allowed_values = [e.name for e in InboundDataFileFormat]
    if f"{stack.stack_name}" == "SalesforceMarketingCloudStack":
        stack_specific_allowed_values = [InboundDataFileFormat.JSONMultiLine.name]
    elif f"{stack.stack_name}" == "S3PushStack":
        stack_specific_allowed_values = [
            InboundDataFileFormat.JSON.name, 
            InboundDataFileFormat.JSONMultiLine.name, 
            InboundDataFileFormat.CSV.name, 
            InboundDataFileFormat.CsvWithHeaderRow.name]

    self.transform_inbound_datafile_format = CfnParameter(stack,
        "InboundDataFileFormat",
        description="Data file format for Databrew dataset",
        allowed_values=stack_specific_allowed_values,
        default=stack_specific_allowed_values[0]
    )
    stack.solutions_template_options.add_parameter(
        self.transform_inbound_datafile_format,
        label="Inbound data file format",
        group=group_name,
    )


def create_glue_database(self, stack) -> None:
    self.cfn_glue_database = glue.CfnDatabase(stack, "CfnGlueDatabase",
        catalog_id=stack.account,
        database_input=glue.CfnDatabase.DatabaseInputProperty(
            name=self.glue_database_name,
            parameters={
                "classification": "parquet"
            }
        )
    )


def create_glue_table(self, stack, transform_bucket_name, transform_bucket_prefix) -> None:
    self.cfn_glue_table = glue.CfnTable(stack, "CfnGlueTable",
        catalog_id=stack.account,
        database_name=self.glue_database_name,
        table_input=glue.CfnTable.TableInputProperty(
            name=self.glue_table_name,
            storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                location=f"s3://{transform_bucket_name}/{transform_bucket_prefix}",
                input_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                output_format="org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                serde_info=glue.CfnTable.SerdeInfoProperty(
                    parameters={},
                    serialization_library="org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                ),
            ),
            parameters={
                "classification": "parquet"
            }
        )
    )
    self.cfn_glue_table.add_dependency(self.cfn_glue_database)


def create_dataset_for_transform(self, stack, inbound_bucket_name, inbound_bucket_prefix) -> None:
    inbound_datafile_format_json_condidition_exp = Fn.condition_equals(
        self.transform_inbound_datafile_format.value_as_string, InboundDataFileFormat.JSON.name)

    inbound_datafile_format_jsonml_condidition_exp = Fn.condition_equals(
        self.transform_inbound_datafile_format.value_as_string, InboundDataFileFormat.JSONMultiLine.name)

    inbound_datafile_format_csv_condidition_exp = Fn.condition_equals(
        self.transform_inbound_datafile_format.value_as_string, InboundDataFileFormat.CSV.name)

    inbound_datafile_bulk_condition_exp = Fn.condition_equals(
        self.transform_inbound_datafile_type_parameter.value_as_string, InboundDataUploadType.Bulk.name)

    json_format_condition = CfnCondition(stack, "json_format_condition", expression=inbound_datafile_format_json_condidition_exp)
    csv_format_condition = CfnCondition(stack, "csv_format_condition", expression=inbound_datafile_format_csv_condidition_exp)

    multiline_json = {
        "Json": {
            "MultiLine": True
        }
    }
    no_multiline_json = {
        "Json": {
            "MultiLine": False
        }
    }
    json_format_options_exp = Fn.condition_if(json_format_condition.logical_id, multiline_json, no_multiline_json)

    no_headers_csv = {
        "Csv": {
            "HeaderRow": False
        }
    }
    header_csv = {
        "Csv": {
            "HeaderRow": True
        }
    }
    csv_format_options_exp = Fn.condition_if(csv_format_condition.logical_id, no_headers_csv, header_csv)

    format_type_condition = CfnCondition(stack, "jsonorcsv",
        expression=Fn.condition_or(inbound_datafile_format_json_condidition_exp, inbound_datafile_format_jsonml_condidition_exp)
    )
    format_type_exp = Fn.condition_if(format_type_condition.logical_id, InboundDataFileFormat.JSON.name, InboundDataFileFormat.CSV.name)

    format_options_exp = Fn.condition_if(format_type_condition.logical_id,
        json_format_options_exp,
        csv_format_options_exp
    )

    inbound_datafile_type_condition = CfnCondition(stack,
        "InboundDatafileBulkCondition",
        expression=inbound_datafile_bulk_condition_exp
    )
    path_object = {
        "FilesLimit": {
            "MaxFiles": 1,
            "OrderedBy": "LAST_MODIFIED_DATE"
        }
    }
    path_expression_object=Fn.condition_if(inbound_datafile_type_condition.logical_id, path_object, json.loads("{}"))

    bucket_suffix = Fn.condition_if(inbound_datafile_type_condition.logical_id, "", "<.*>").to_string()

    self.cfn_dataset = CfnResource(
        stack,
        "BrewCfnDataset",
        type="AWS::DataBrew::Dataset",
        properties={
            "Name": self.dataset_name,
            "Format": format_type_exp,
            "FormatOptions": format_options_exp,
            "Input": {
                "S3InputDefinition": {
                    "Bucket": inbound_bucket_name,
                    "Key": f"{inbound_bucket_prefix}{bucket_suffix}"
                }
            },
            "PathOptions": path_expression_object
        }
    )


def create_policies_for_databrew_transform(self, inbound_bucket_arn: str, transform_bucket_arn: str, stack_region,
                                           stack_account_id) -> iam.PolicyDocument:
    glue_actions = [
        "glue:List*",
        "glue:Get*",
        "glue:Put*",
        "glue:Update*",
    ]
    s3_list_action = "s3:List*"
    s3_get_action = "s3:Get*"
    inbound_bucket_policy_statement = iam.PolicyStatement(
        actions=[s3_list_action, s3_get_action],
        resources=[
            f"{inbound_bucket_arn}",
            f"{inbound_bucket_arn}/*"
        ],
        effect=iam.Effect.ALLOW,
    )
    transform_bucket_policy_statement = iam.PolicyStatement(
        actions=[
            s3_list_action,
            s3_get_action,
            "s3:Put*",
            "s3:Delete*",
        ],
        resources=[
            f"{transform_bucket_arn}",
            f"{transform_bucket_arn}/*"
        ],
        effect=iam.Effect.ALLOW,
    )
    glue_database_policy_statement = iam.PolicyStatement(
        actions=glue_actions,
        resources=[
            f"arn:aws:glue:{stack_region}:{stack_account_id}:database/{self.glue_database_name}"
        ],
        effect=iam.Effect.ALLOW,
    )
    glue_table_policy_statement = iam.PolicyStatement(
        actions=glue_actions,
        resources=[
            f"arn:aws:glue:{stack_region}:{stack_account_id}:table/{self.glue_database_name}/{self.glue_table_name}",
        ],
        effect=iam.Effect.ALLOW,
    )
    glue_catalog_policy_statement = iam.PolicyStatement(
        actions=glue_actions,
        resources=[
            f"arn:aws:glue:{stack_region}:{stack_account_id}:catalog"
        ],
        effect=iam.Effect.ALLOW,
    )
    databrew_logs_policy_statement = iam.PolicyStatement(
        actions=[
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
        ],
        resources=[
            f"arn:aws:logs:{stack_region}:{stack_account_id}:log-group:/aws-glue-databrew/*"
        ],
        effect=iam.Effect.ALLOW,
    )
    databrew_secrets_policy = iam.PolicyStatement(
        actions=[
            "secretsmanager:GetSecretValue",
            "secretsmanager:ListSecrets"
        ],
        resources=[
            f"arn:aws:secretsmanager:{stack_region}:{stack_account_id}:secret:{DATABREW_CUSTOM_SECRETS_PREFIX}*",
            f"arn:aws:secretsmanager:{stack_region}:{stack_account_id}:secret:databrew!default-*"
        ],
        effect=iam.Effect.ALLOW
    )
    databrew_kms_policy = iam.PolicyStatement(
        actions=[
            "kms:ListKeys",
            "kms:ListAliases",
            "kms:GenerateDataKey",
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:ReEncrypt*",
        ],
        resources=[
            f"arn:aws:kms:{stack_region}:{stack_account_id}:key/*"
        ],
        effect=iam.Effect.ALLOW
    )
    databrew_custom_entities_policy = iam.PolicyStatement(
        actions=[
            "glue:BatchGetCustomEntityTypes"
        ],
        resources=[ 
            "*"
        ],
        effect=iam.Effect.ALLOW
    )

    return iam.PolicyDocument(
        statements=[
            inbound_bucket_policy_statement,
            transform_bucket_policy_statement,
            glue_database_policy_statement,
            glue_table_policy_statement,
            glue_catalog_policy_statement,
            databrew_logs_policy_statement,
            databrew_secrets_policy,
            databrew_kms_policy,
            databrew_custom_entities_policy
        ]
    )


def create_databrew_transform_iam_role(stack, policy_document) -> iam.Role:
    return iam.Role(
        stack,
        "DatabrewTransformIamRole",
        assumed_by=iam.ServicePrincipal("databrew.amazonaws.com"),
        inline_policies={
            "DatabrewTransformRolePolicy": policy_document
        }
    )


def create_profile_job_for_dataset(self, stack, transform_bucket_name, transform_bucket_prefix, role_arn,
                                   encryption_key_arn) -> None:
    transform_bucket_prefix = transform_bucket_prefix or None
    self.cfn_job_profile_type = databrew.CfnJob(stack, "BrewCfnProfileJob",
        name=self.profile_job_name,
        role_arn=role_arn,
        type="PROFILE",
        dataset_name=self.dataset_name,
        encryption_key_arn=encryption_key_arn,
        encryption_mode="SSE-KMS",
        output_location=databrew.CfnJob.OutputLocationProperty(
            bucket=transform_bucket_name,
            key=transform_bucket_prefix,
        ),
        profile_configuration=databrew.CfnJob.ProfileConfigurationProperty(
            entity_detector_configuration=databrew.CfnJob.EntityDetectorConfigurationProperty(
                entity_types=["USA_ALL"],
            )
        )
    )
    self.cfn_job_profile_type.add_dependency(self.cfn_dataset)
    self.cfn_job_profile_type.node.add_dependency(self.recipe_lambda_custom_resource)


def create_policy_statements_for_custom_lambda(self, stack, stack_region, stack_account_id, inbound_bucket_name,
                                               inbound_bucket_prefix) -> list[iam.PolicyStatement]:
    s3_list_action = "s3:List*"
    s3_get_action = "s3:Get*"
    databrew_recipe_policy_statement = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=[
            "databrew:CreateRecipe",
            "databrew:UpdateRecipe",
            "databrew:Delete*",
            "databrew:ListRecipeVersions",
        ],
        resources=[
            f"arn:aws:databrew:{stack_region}:{stack_account_id}:recipe/{self.recipe_name}"
        ]
    )

    databrew_inbound_bucket_prefix_statement = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=[
            s3_list_action,
            s3_get_action,
            "s3:Put*",
        ],
        resources=[
            f"arn:aws:s3:::{inbound_bucket_name}/{inbound_bucket_prefix}*",
        ]
    )

    transform_recipe_file_empty_condition = CfnCondition(stack,
        "RecipeFileEmptyCondition",
        expression=Fn.condition_equals(self.transform_recipe_file_location_parameter.value_as_string, "")
    )

    recipe_file_policy_statement = iam.PolicyStatement(
        effect=iam.Effect.ALLOW,
        actions=[
            s3_list_action,
            s3_get_action,
        ],
        resources=[
            Fn.condition_if(transform_recipe_file_empty_condition.logical_id,
                "arn:aws:s3:::NON-EXI$TANT-PLACEH@LDER", # placeholder for non-empty value, never accessed
                f"arn:aws:s3:::{self.transform_recipe_file_location_parameter.value_as_string}"
            ).to_string()
        ]
    )

    return [
        databrew_recipe_policy_statement,
        databrew_inbound_bucket_prefix_statement,
        recipe_file_policy_statement
    ]


def create_recipe_lambda_custom_function(self, stack, recipe_lambda_policy):
    """
    This function is responsible for creating/updating/deleting the databrew recipe
    based on TransformRecipeFileLocation.
    Additionally places a sample file in the inbound bucket on create
    """

    self.recipe_lambda_custom_resource_function = SolutionsPythonFunction(
        stack,
        "TransformCustomLambdaFunction",
        LAMBDA_PATH / "custom_resource" / "transform" / "recipe_from_s3.py",
        "event_handler",
        runtime=lambdaf.Runtime.PYTHON_3_9,
        description="Lambda function for custom resource for creating/updating/deleting the databrew recipe",
        timeout=Duration.minutes(5),
        memory_size=256,
        architecture=lambdaf.Architecture.ARM_64,
        layers=[PowertoolsLayer.get_or_create(stack),
                SolutionsLayer.get_or_create(stack)],
    )
    self.recipe_lambda_custom_resource_function.add_environment(
        "SOLUTION_ID", stack.node.try_get_context("SOLUTION_ID")
    )
    self.recipe_lambda_custom_resource_function.add_environment(
        "SOLUTION_VERSION", stack.node.try_get_context("SOLUTION_VERSION")
    )
    recipe_lambda_policy.attach_to_role(self.recipe_lambda_custom_resource_function.role)


def create_recipe_lambda_custom_resource(self, stack, inbound_bucket_name, inbound_bucket_prefix):
    """
    This function creates the custom resource for populating recipe in databrew
    """
    self.recipe_lambda_custom_resource = CustomResource(stack,
        "TransformCustomResource",
        service_token=self.recipe_lambda_custom_resource_function.function_arn,
        properties={
            "recipe_s3_location": self.transform_recipe_file_location_parameter.value_as_string,
            "recipe_name": self.recipe_name,
            "inbound_bucket_name": inbound_bucket_name,
            "inbound_bucket_prefix": inbound_bucket_prefix,
        },
    )
    self.recipe_lambda_custom_resource.node.add_dependency(self.recipe_lambda_iam_policy)


def create_recipe_job_ref_project(self, stack, transform_bucket_name, transform_bucket_prefix, role_arn, encryption_key_arn) -> None:
    transform_bucket_prefix = transform_bucket_prefix or None
    self.cfn_job_recipe_type = databrew.CfnJob(stack, "BrewCfnRecipeJob",
        name=self.recipe_job_name,
        role_arn=role_arn,
        type="RECIPE",
        project_name=self.cfn_project.name,
        encryption_key_arn=encryption_key_arn,
        encryption_mode="SSE-KMS",
        data_catalog_outputs=[databrew.CfnJob.DataCatalogOutputProperty(
            database_name=self.glue_database_name,
            table_name=self.glue_table_name,
            overwrite=False,
            s3_options=databrew.CfnJob.S3TableOutputOptionsProperty(
                location=databrew.CfnJob.S3LocationProperty(
                    bucket=transform_bucket_name,
                    key=transform_bucket_prefix,
                )
            )
        )]
    )
    self.cfn_job_recipe_type.add_dependency(self.cfn_project)


def create_project_with_recipe(self, stack, role_arn) -> None:
    self.cfn_project = databrew.CfnProject(stack, "BrewCfnProject",
        dataset_name=self.dataset_name,
        name=self.project_name,
        recipe_name=self.recipe_name,
        role_arn=role_arn,
        sample=databrew.CfnProject.SampleProperty(
            type="RANDOM",
            size=250
        )
    )
    self.cfn_project.node.add_dependency(self.cfn_dataset)
    self.cfn_project.node.add_dependency(self.recipe_lambda_custom_resource)


def create_stack_outputs(self, stack) -> None:
    CfnOutput(stack, "DataBrewRecipeJob",
                value=f"https://{Aws.REGION}.console.aws.amazon.com/databrew/home?region={Aws.REGION}#job-details?job={self.cfn_job_recipe_type.name}")
    CfnOutput(stack, "Glue Data Catalog Table", value=self.glue_table_name)
    CfnOutput(stack, "Glue Data Catalog Database", value=self.cfn_glue_table.database_name)


def databrew_role_apply_nag_suppressions(databrew_iam_role) -> None:
    nag_suppression_reason = "IAM entity contains wildcard permissions"
    NagSuppressions.add_resource_suppressions(
        databrew_iam_role,
        [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:List*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:Get*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:Put*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:Delete*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ['Resource::<inboundbucketFA352838.Arn>/*']
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Resource::*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Resource::<transformbucket674563F2.Arn>/*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::glue:Get*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::glue:Put*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::glue:List*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::glue:Update*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Resource::arn:aws:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws-glue-databrew/*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::kms:GenerateDataKey*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": [f"Resource::arn:aws:secretsmanager:<AWS::Region>:<AWS::AccountId>:secret:{DATABREW_CUSTOM_SECRETS_PREFIX}*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Resource::arn:aws:secretsmanager:<AWS::Region>:<AWS::AccountId>:secret:databrew!default-*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Resource::arn:aws:kms:<AWS::Region>:<AWS::AccountId>:key/*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::kms:ReEncrypt*"]
            },
        ]
    )


def recipe_policy_apply_nag_suppresions(recipe_lambda_iam_policy) -> None:
    nag_suppression_reason = "IAM entity contains wildcard permissions"
    NagSuppressions.add_resource_suppressions(
        recipe_lambda_iam_policy,
        [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:Put*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:List*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:Get*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ['Resource::arn:aws:s3:::<inboundbucketFA352838>/{"Fn::If":["InboundBucketPrefixCondition","inbound/",{"Fn::Join":["",[{"Ref":"AWS::StackName"},"-flow/"]]}]}*']
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::s3:Update*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Action::databrew:Delete*"]
            },
            {
                "id": 'AwsSolutions-IAM5',
                "reason": nag_suppression_reason,
                "appliesTo": ["Resource::arn:aws:s3:::<inboundbucketFA352838>/<InboundBucketPrefix>*"]
            }
        ],
    )


def object_remove_policy_apply_nag_suppresions(object_remove_lambda_iam_policy):
    NagSuppressions.add_resource_suppressions(
        object_remove_lambda_iam_policy,
        [
            {
                "id": 'AwsSolutions-IAM5',
                "reason": "IAM entity contains wildcard permissions",
                "appliesTo": ['Resource::arn:aws:s3:::<inboundbucketFA352838>/{"Fn::If":["InboundBucketPrefixCondition","inbound/",{"Fn::Join":["",[{"Ref":"AWS::StackName"},"-flow/"]]}]}*']
            },
        ]
    )


def custom_lambda_resource_function_nag_suppression(*custom_lambda_resource_functions) -> None:
    for custom_lambda_resource_function in custom_lambda_resource_functions:
        NagSuppressions.add_resource_suppressions(
            custom_lambda_resource_function.role.node.default_child,
            [
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": '* Resource needed by logs',
                    "appliesTo": ["Resource::arn:<AWS::Partition>:logs:<AWS::Region>:<AWS::AccountId>:log-group:/aws/lambda/*"]
                }
            ]
        )
        