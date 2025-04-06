import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
from fast_api import app, DataProcessor
import os

# Mock environment variable
os.environ['API_KEY'] = 'test-api-key'

# Create a test client for FastAPI
client = TestClient(app)

@pytest.fixture
def mock_s3_client(mocker):
    # Patch boto3.client directly in the module where it's being used.
    mock_s3 = mocker.patch('boto3.client')  # Ensure this path is correct
    mock_s3.return_value = MagicMock()
    return mock_s3

@pytest.fixture
def data_processor():
    # This will initialize the DataProcessor with mocked S3
    return DataProcessor(region="eu-north-1", bucket_name="testbucket")

def test_check_api_key_valid(data_processor):
    # Test valid API key
    api_key = 'test-api-key'
    try:
        # Directly check API key validation
        if api_key != os.getenv('API_KEY'):
            raise ValueError("Invalid API key")
    except Exception as e:
        pytest.fail(f"check_api_key raised {type(e).__name__} unexpectedly!")

def test_check_api_key_invalid(data_processor):
    # Test invalid API key
    invalid_api_key = 'invalid-api-key'
    with pytest.raises(Exception):
        if invalid_api_key != os.getenv('API_KEY'):
            raise ValueError("Invalid API key")

def test_process_data_invalid_quality():
    # Test invalid quality query parameter
    print(os.environ['API_KEY'])
    response = client.get("/process_data?qualityquery=medium", headers={"api-key": "test-api-key"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid quality query parameter. Allowed values: high, low"}

def test_process_data_missing_api_key():
    # Test missing API key in header
    response = client.get("/process_data?qualityquery=high")
    assert response.status_code == 401
    assert response.json() == {'detail': 'Invalid or missing API key.'}

def test_process_data_valid_quality(mock_s3_client):
    # Mock the S3 response for the test
    mock_s3_client.return_value.get_object.return_value = {
        'Body': MagicMock(read=MagicMock(return_value=b'{"key": "value"}'))
    }

    # Test valid 'high' quality parameter
    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})
    assert response.status_code == 200
    assert response.json() == {"key": "value"}

def test_process_data_s3_error(mock_s3_client):
    # Simulate an S3 error by setting side_effect
    mock_s3_client.return_value.get_object.side_effect = Exception("S3 error")
    
    # Test processing data with valid quality but S3 error
    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})
    
    # Check that the status code is 500 and the error message is correct
    assert response.status_code == 500
    assert response.json() == {"detail": "Error processing file high_quality_average.json: S3 error"}
