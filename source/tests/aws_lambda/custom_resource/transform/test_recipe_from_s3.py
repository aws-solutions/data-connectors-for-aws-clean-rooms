import pytest
import io
import json
from botocore.response import StreamingBody

from unittest.mock import Mock
from aws_lambda.custom_resource.transform.recipe_from_s3 import on_create_or_update, on_delete, \
    logger as lambda_function_logger
from aws_solutions.core.helpers import get_service_client, _helpers_service_clients


@pytest.fixture()
def _mock_databrew_client():
    client = get_service_client('databrew')
    client.create_recipe = Mock(return_value={'Name': 'databrew_recipe_name'})
    client.update_recipe = Mock(return_value={'Name': 'update_recipe_name'})
    client.list_recipe_versions = Mock(
        return_value={"Recipes": [{"RecipeVersion": "01"}, {"RecipeVersion": "LATEST_WORKING"}]})
    client.delete_recipe_version = Mock(return_value={'Name': 'delete_recipe_name'})
    return client


@pytest.fixture()
def _mock_streaming_body():
    body_json = {
        'Body': [
            {'Object': 'transform_recipe_content'}
        ]
    }
    body_encoded = json.dumps(body_json).encode()
    return StreamingBody(
        io.BytesIO(body_encoded),
        len(body_encoded)
    )


@pytest.fixture()
def _mock_s3_client(_mock_streaming_body):
    client = get_service_client('s3')
    client.upload_file = Mock()
    client.get_object = Mock(return_value={"Body": _mock_streaming_body})
    return client


@pytest.fixture()
def mock_databrew_and_s3(monkeypatch, _mock_databrew_client, _mock_s3_client):
    monkeypatch.setitem(_helpers_service_clients,
                        'databrew', _mock_databrew_client)
    monkeypatch.setitem(_helpers_service_clients,
                        's3', _mock_s3_client)
    monkeypatch.setattr(lambda_function_logger, 'error', Mock())


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties":
                {
                    'recipe_s3_location': "bucket/recipe.json",
                    'inbound_bucket_prefix': "inbound/",
                    'inbound_bucket_name': "myBucket",
                    'recipe_name': 'recipe.json'
                },
            "RequestType": "Create",

        }
    ],
)
def test_on_create(lambda_event, mock_databrew_and_s3):
    on_create_or_update(lambda_event, None)
    _helpers_service_clients['s3'].get_object.assert_called_once()
    _helpers_service_clients['s3'].upload_file.assert_called_once()
    _helpers_service_clients["databrew"].create_recipe.assert_called_once()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties":
                {
                    'recipe_s3_location': "bucket/recipe.json",
                    'inbound_bucket_prefix': "inbound/",
                    'inbound_bucket_name': "myBucket",
                    'recipe_name': 'recipe.json'
                },
            "RequestType": "Update",

        }
    ],
)
def test_on_update(lambda_event, mock_databrew_and_s3):
    on_create_or_update(lambda_event, None)
    _helpers_service_clients["databrew"].update_recipe.assert_called_once()


@pytest.mark.parametrize(
    "lambda_event",
    [
        {
            "ResourceProperties":
                {
                    'recipe_s3_location': "bucket/recipe.json",
                    'inbound_bucket_prefix': "inbound/",
                    'inbound_bucket_name': "myBucket",
                    'recipe_name': 'recipe.json'
                },
            "RequestType": "Update",
            "PhysicalResourceId": "01",
        }
    ],
)
def test_on_delete(lambda_event, mock_databrew_and_s3):
    on_delete(lambda_event, None)
    _helpers_service_clients["databrew"].list_recipe_versions.assert_called_once()
    _helpers_service_clients["databrew"].delete_recipe_version.assert_called()
