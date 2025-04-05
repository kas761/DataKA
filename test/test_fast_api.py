import pytest
from fastapi.testclient import TestClient
from fastapi import HTTPException
from unittest.mock import patch, MagicMock
from fast_api import app, check_api_key, process_json_data  # Assuming your FastAPI code is in app.py
import os
from unittest import mock
from moto import mock_aws
import boto3
import json


# Test client for FastAPI
client = TestClient(app)
# Define the bucket name and a sample key
BUCKET_NAME = "test-bucket"
SAMPLE_KEY = "some_file.json"
SAMPLE_CONTENT = {"data": "test data"}

@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with mock.patch.dict(os.environ, {"API_KEY": "test_api_key"}):
        yield

@pytest.fixture()
def mock_s3():
    with mock_aws():
        s3_client = boto3.client('s3', region_name='eu-north-1')
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=SAMPLE_KEY,
            Body=json.dumps(SAMPLE_CONTENT)
        )
        yield s3_client

@pytest.fixture
def s3_client(mock_s3):
    # Initialize the mock S3 client
    return mock_s3

def test_check_api_key_valid():
    try:
        check_api_key('test_api_key')
    except HTTPException:
        pytest.fail("check_api_key raised HTTPException unexpectedly!")


def test_check_api_key_invalid():
    with pytest.raises(HTTPException):
        check_api_key('invalid-api-key')

def test_process_json_data_success(mock_s3):
    result = process_json_data(SAMPLE_KEY)
    assert result == {"key": "value"}

def test_process_data_valid(s3_client):
    response = client.get(
        "/process_data?qualityquery=high", 
        headers={"X-Api-Key": "test_api_key"}
    )
    assert response.status_code == 200
    assert response.json() == {"data": "test data"}


def test_process_data_invalid_quality():
    response = client.get(
        "/process_data?qualityquery=medium", 
        headers={"X-Api-Key": "test_api_key"}
    )
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid quality query parameter. Allowed values: high, low"}

def test_process_json_data_exception(mock_s3):
    with patch('boto3.client') as mock_boto_client:
        # Simulate S3 fetch failure by making get_object throw an exception
        mock_s3_instance = MagicMock()
        mock_boto_client.return_value = mock_s3_instance
        mock_s3_instance.get_object.side_effect = Exception("S3 fetch failed")

        with pytest.raises(HTTPException):
            process_json_data("high_quality_average.json")


def test_process_data_missing_quality():
    response = client.get(
        "/process_data", 
        headers={"X-Api-Key": "test_api_key"}
    )
    assert response.status_code == 422
    assert response.json() == {'detail': [{'input': None, 'loc': ['query', 'qualityquery'], 'msg': 'Field required', 'type': 'missing'}]}
