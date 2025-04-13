import pytest
from fastapi.testclient import TestClient
from fast_api import app, get_data_processor
from unittest.mock import MagicMock
import os
from fastapi import HTTPException

# Set the API key environment variable
os.environ['API_KEY'] = 'test-api-key'

# Create a test client for FastAPI
client = TestClient(app)


# === Test: Valid API key ===
def test_check_api_key_valid():
    mock_processor = MagicMock()
    mock_processor.get_file_key.return_value = "high_quality_average.json"
    mock_processor.process_json_data.return_value = {"key": "value"}

    app.dependency_overrides[get_data_processor] = lambda: mock_processor

    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})

    assert response.status_code == 200
    assert response.json() == {"key": "value"}

    app.dependency_overrides.clear()


# === Test: Invalid or missing API key ===
def test_check_api_key_invalid_or_missing():
    response = client.get("/process_data?qualityquery=high", headers={"api-key": "invalid-key"})
    assert response.status_code == 401
    assert response.json() == {"detail": "Invalid or missing API key."}


# === Test: Invalid quality query ===
def test_process_data_invalid_quality():
    response = client.get("/process_data?qualityquery=medium", headers={"api-key": "test-api-key"})
    assert response.status_code == 400
    assert response.json() == {"detail": "Invalid quality parameter. Allowed values: high, low"}


# === Test: Valid quality query and mocked S3 logic ===
def test_process_data_valid_quality():
    mock_processor = MagicMock()
    mock_processor.get_file_key.return_value = "high_quality_average.json"
    mock_processor.process_json_data.return_value = {"key": "value"}

    app.dependency_overrides[get_data_processor] = lambda: mock_processor

    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})

    assert response.status_code == 200
    assert response.json() == {"key": "value"}

    app.dependency_overrides.clear()


def test_process_data_s3_error():
    mock_processor = MagicMock()
    mock_processor.get_file_key.return_value = "high_quality_average.json"
    mock_processor.process_json_data.side_effect = HTTPException(status_code=500, detail="Error processing file: S3 error")

    app.dependency_overrides[get_data_processor] = lambda: mock_processor

    response = client.get("/process_data?qualityquery=high", headers={"api-key": "test-api-key"})

    assert response.status_code == 500
    assert response.json() == {"detail": "Error processing file: S3 error"}

    app.dependency_overrides.clear()
