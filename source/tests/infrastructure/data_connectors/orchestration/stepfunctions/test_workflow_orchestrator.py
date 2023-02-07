from pathlib import Path
import aws_cdk as cdk
import pytest
from aws_cdk.assertions import Match, Template, Capture
from aws_solutions.cdk import CDKSolution
from aws_solutions.cdk.stack import SolutionStack
from data_connectors.orchestration.stepfunctions.workflow_orchestrator import WorkflowOrchestrator
from aws_cdk.aws_sns import Topic
import aws_cdk.aws_dynamodb as dynamodb
from aws_cdk.aws_dynamodb import Table
from aws_cdk import CustomResource


@pytest.fixture(scope="module")
def mock_solution():
    path = Path(__file__).parent / ".." / ".." / "cdk.json"
    return CDKSolution(cdk_json_path=path)


@pytest.fixture(scope="module")
def synth_template(mock_solution):
    app = cdk.App(context=mock_solution.context.context)
    stack = SolutionStack(app,
                          "TestWorkflowOrchestrator",
                          description="Empty Stack for Testing",
                          template_filename="test-workflow-orchestrator.template")

    mock_sns_topic = Topic(stack, "UnitTestTopic")
    mock_dynamodb_table = Table(stack,
                                "UnitTestDynamodbTable",
                                partition_key=dynamodb.Attribute(name="watching_key",
                                                                 type=dynamodb.AttributeType.STRING)
                                )
    mock_custom_resource = CustomResource(stack, "UnitTestRecipeCustomResource", service_token="UnitTestServiceToken")

    WorkflowOrchestrator(
        stack, "TestWorkflowOrchestrator",
        recipe_name="UnitTestRecipe",
        s3_bucket_name="UnitTestS3Bucket",
        dataset_name="UnitTestDataset",
        recipe_bucket_name="UnitTestRecipeBucket",
        recipe_job_name="UnitTestRecipeJob",
        sns_topic=mock_sns_topic,
        dynamodb_table=mock_dynamodb_table,
        recipe_lambda_custom_resource=mock_custom_resource
    )
    synth_template = Template.from_stack(stack)
    yield synth_template


def test_base_workflow_creation(synth_template):
    synth_template.resource_count_is("AWS::StepFunctions::StateMachine", 1)


def test_invoke_lambda_run_brew_jobs_creation(synth_template):
    role_definition_capture = Capture()
    states_definition_capture = Capture()
    synth_template.has_resource_properties(
        "AWS::StepFunctions::StateMachine",
        {
            "RoleArn": {
                "Fn::GetAtt": [role_definition_capture, "Arn"]
            },
            "DefinitionString": states_definition_capture,
        }
    )
    states_definition = str(states_definition_capture.as_object()['Fn::Join'][1])

    payload = "\"Payload\":{\"task_token.$\":\"$$.Task.Token\",\"brew_job_name\":\"UnitTestRecipeJob\"}"
    assert payload in states_definition

    on_catch = "\"Catch\":[{\"ErrorEquals\":[\"States.TaskFailed\"],\"Next\":\"Databrew Job Fail Notification\"}]"
    assert on_catch in states_definition
