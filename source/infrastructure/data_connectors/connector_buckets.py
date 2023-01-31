# pylint: disable=line-too-long,too-few-public-methods
"""
This module is responsible for creating the workflow orchestration resources.
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

from aws_cdk import Stack, Aws
from aws_cdk import CfnOutput, RemovalPolicy, CfnCondition, Fn
from aws_cdk import aws_s3, aws_iam
from cdk_nag import NagSuppressions

INBOUND_BUCKET_PREFIX_S3_PUSH = "inbound/"
TRANSFORM_BUCKET_PREFIX = "transform/"

class ConnectorBuckets:
    """
    This class encapsulates the bucket resources needed by the base stack
    """

    def create_inbound_bucket(self, stack: Stack) -> None:
        """
        This function is responsible for creating the bucket and prefix for inbound data
        """
        self.inbound_bucket = aws_s3.Bucket(
            stack,
            "inbound-bucket",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
            server_access_logs_bucket=self.solution_logging_bucket,
            server_access_logs_prefix="inbound-logs/",
            versioned=True
        )

        # adjust prefix based on the stack type
        inbound_prefix_condition = CfnCondition(stack,
            "InboundBucketPrefixCondition",
            expression=Fn.condition_equals(
                stack.stack_name,
                "S3PushStack"
            )
        )

        self.inbound_bucket_prefix = Fn.condition_if(
            inbound_prefix_condition.logical_id, 
            INBOUND_BUCKET_PREFIX_S3_PUSH,
            f"{Aws.STACK_NAME}-flow/"
        ).to_string()

    def create_transform_bucket(self, stack: Stack) -> None:
        """
        This function is responsible for creating the bucket and prefix for transformed data
        """
        self.transform_bucket = aws_s3.Bucket(
            stack,
            "transform-bucket",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
            server_access_logs_bucket=self.solution_logging_bucket,
            server_access_logs_prefix="transform-logs/",
            versioned=True
        )
        self.transform_bucket_prefix = TRANSFORM_BUCKET_PREFIX

    def create_access_logging_bucket(self, stack: Stack) -> None:
        """
        This function is responsible for creating the access logging bucket used
        by the inbound and transformed data buckets
        """
        self.solution_logging_bucket = aws_s3.Bucket(
            stack,
            "solution-logging",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
            versioned=True
        )

    def create_iam_group(self, stack: Stack) -> None:
        """
        This function creates an IAM Group that can be attached to an IAM User for generating keys
        with sufficient permissions to list, get, and create objects in the inbound data bucket, and
        work with KMS keys to encrypt those objects
        """
        bucket_access_policy_statement = aws_iam.PolicyStatement(
            actions=[
                "s3:put*",
                "s3:get*",
                "s3:list*",
                "s3:*multipart*",
            ],
            resources=[
                f"{self.inbound_bucket.bucket_arn}",
                f"{self.inbound_bucket.bucket_arn}/*"
            ],
        )
        kms_encrypt_policy_statement = aws_iam.PolicyStatement(
            actions=[
                "kms:encrypt*",
                "kms:list*",
                "kms:get*",
                "kms:generate*",
                "kms:describe*",
            ],
            resources=["*"],
        )
        self.inbound_bucket_access_policy = aws_iam.Policy(
            stack,
            "InboundBucketAccessPolicy",
            statements=[
                bucket_access_policy_statement, kms_encrypt_policy_statement
            ]
        )
        # set to RETAIN in case this is assigned to one or more IAM Users
        self.iam_group = aws_iam.Group(stack,
            "InboundBucketAccessGroup"
        )
        self.iam_group.attach_inline_policy(self.inbound_bucket_access_policy)
        self.iam_group.apply_removal_policy(RemovalPolicy.RETAIN)

        NagSuppressions.add_resource_suppressions(
            self.inbound_bucket_access_policy,
            [
                {
                    "id": 'AwsSolutions-IAM5',
                    "reason": '* Actions applied to specific resource',
                    "appliesTo": ["Action::kms:generate*"]
                },
            ]
        )

    def create_stack_outputs(self, stack: Stack) -> None:
        CfnOutput(
            stack,
            "BucketInboundData",
            value=
            f"https://s3.console.aws.amazon.com/s3/buckets/{self.inbound_bucket.bucket_name}/{self.inbound_bucket_prefix}",
        )


    def create_cdk_nag_suppressions(self) -> None:
        # KMS and S3 for IAM Group
        NagSuppressions.add_resource_suppressions(
            self.inbound_bucket_access_policy,
            [
                {
                    "id":
                    "AwsSolutions-IAM5",
                    "reason":
                    "Not known in advance which customer-managed keys will be needed by solution",
                    "appliesTo": [
                        "Action::kms:encrypt*", "Action::kms:list*",
                        "Action::kms:get*", "Action::kms:describe*",
                        "Action::kms:generate*",
                        "Resource::*"
                    ]
                },
                {
                    "id":
                    "AwsSolutions-IAM5",
                    "reason":
                    "Bucket permissions needed by external data providers for bulk and incremental push",
                    "appliesTo": [
                        "Action::s3:put*", "Action::s3:get*",
                        "Action::s3:list*", "Action::s3:*multipart*"
                    ]
                },
                {
                    "id": "AwsSolutions-IAM5",
                    "reason":
                    "Access to entire bucket by external data providers is required",
                    "appliesTo": ["Resource::<inboundbucketFA352838.Arn>/*"]
                },
            ],
        )
        # logging bucket
        NagSuppressions.add_resource_suppressions(
            self.solution_logging_bucket,
            [{
                "id": "AwsSolutions-S1",
                "reason":
                "This is the access logs bucket for other solution buckets"
            }],
        )

    def __init__(self, stack: Stack):
        self.create_access_logging_bucket(stack=stack)
        self.create_inbound_bucket(stack=stack)
        self.create_transform_bucket(stack=stack)
        self.create_stack_outputs(stack=stack)
        self.create_iam_group(stack=stack)
        self.create_cdk_nag_suppressions()
