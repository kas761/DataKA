import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
from fast_api import app, DataProcessor
import boto3
from moto import mock_aws
import os
import json

# Mock environment variable
os.environ['API_KEY'] = 'test-api-key'

# Create a test client for FastAPI
client = TestClient(app)

@pytest.fixture
def mock_s3_setup():
    with mock_aws():
        # Specify the region
        test_region = "eu-north-1"  # You can use any region that moto supports
        test_s3_client = boto3.client('s3', region_name=test_region)
        test_bucket_name = "my-test-bucket"
        test_s3_client.create_bucket(Bucket=test_bucket_name, CreateBucketConfiguration={
            'LocationConstraint': test_region
        })
        json_data = {"key": "value"}
        test_s3_client.put_object(Bucket=test_bucket_name, Key="my_json_file.json", Body=json.dumps(json_data))
        yield test_s3_client, test_bucket_name, test_region

@pytest.fixture
def data_processor(mock_s3_setup):
    test_s3_client, test_bucket_name, test_region = mock_s3_setup  # Receive the client and bucket name
    return DataProcessor(s3_client=test_s3_client, region=test_bucket_name, bucket_name=test_region)

def test_check_api_key_valid():
    # Valid API key
    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})
    assert response.status_code == 200

def test_check_api_key_invalid_or_missing():
    # Invalid API key
    response = client.get("/process_data?qualityquery=high", headers={"api-key": "invalid-test-api-key"})
    assert response.status_code == 401

def test_process_data_invalid_quality():
    # Invalid quality query parameter
    response = client.get("/process_data?qualityquery=medium", headers={"api-key": "test-api-key"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid quality query parameter. Allowed values: high, low"}

def test_process_data_valid_quality(mock_s3_setup):
    # Mock the S3 response
    s3_client, _, _ = mock_s3_setup
    s3_client.get_object = MagicMock(return_value={'Body': MagicMock(read=MagicMock(return_value=b'{"key": "value"}'))})

    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})
    assert response.status_code == 200
    assert response.json() == {"key": "value"}

def test_process_data_s3_error(mock_s3_setup):
    # Simulate S3 error
    s3_client, _, _ = mock_s3_setup
    s3_client.get_object = MagicMock(side_effect=Exception("S3 error"))
    
    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})
    
    assert response.status_code == 500
    assert response.json() == {"detail": "Error processing file high_quality_average.json: S3 error"}
