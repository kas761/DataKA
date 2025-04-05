import pytest
import json
from unittest.mock import patch, MagicMock
import boto3
from moto import mock_aws
import pandas as pd
from io import StringIO
from aws_lambda import S3Utils
from io import BytesIO

# Sample CSV data to use in tests
CSV_DATA = "col1,col2,col3\n1,2,3\n4,5,6\n7,8,9"

@pytest.fixture
def s3_setup():
    with mock_aws():
        # Setup S3 bucket and upload test files
        s3_client = boto3.client('s3', region_name='eu-north-1')
        bucket_name = 'dataka'

        # Create bucket with a location constraint (for regions like 'eu-north-1')
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'}
        )

        # Upload test CSV to S3
        s3_client.put_object(Bucket=bucket_name, Key='winequality-red.csv', Body=CSV_DATA.encode('utf-8'))
        s3_client.put_object(Bucket=bucket_name, Key='winequality-white.csv', Body=CSV_DATA.encode('utf-8'))
        
        yield s3_client

@pytest.fixture
def secrets_manager_setup():
    with mock_aws():
        # Setup Secrets Manager with a mock secret
        secrets_client = boto3.client('secretsmanager', region_name='eu-north-1')
        secret_value = json.dumps({
            "AWS_ACCOUNT_ID": "123456789012",
            "LAMBDA_ROLE_ARN": "arn:aws:iam::123456789012:role/lambda-role"
        })
        secrets_client.create_secret(Name='secret', SecretString=secret_value)
        yield secrets_client

@mock_aws
def test_lambda_handler(s3_setup, secrets_manager_setup):
    # Initialize S3Utils and mock necessary methods
    s3_utils = S3Utils(bucket_name='dataka')

    # Mock the event that triggers the Lambda function
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "dataka"
                    },
                    "object": {
                        "key": "winequality-red.csv"
                    }
                }
            }
        ]
    }

    # Mock the S3 get_object method to return the CSV content as bytes
    with patch.object(s3_utils.s3_client, 'get_object', return_value={
        'Body': BytesIO(CSV_DATA.encode('utf-8'))  # Ensure content is returned as bytes
    }):

        # Mock get_secret method to return the mock secret
        with patch.object(s3_utils.secrets_manager_client, 'get_secret_value', return_value={
            'SecretString': json.dumps({
                'AWS_ACCOUNT_ID': '123456789012',
                'LAMBDA_ROLE_ARN': 'arn:aws:iam::123456789012:role/lambda-role'
            })
        }):

            # Execute the Lambda handler
            result = s3_utils.lambda_handler(event, None)

            # Assertions to ensure the Lambda function processes correctly
            assert result['statusCode'] == 200
            assert 'Successfully processed' in result['body']
            assert 'winequality-red.csv' in result['body']

            # Verifying that pandas processed the CSV data correctly
            df = pd.read_csv(BytesIO(CSV_DATA.encode('utf-8')))
            assert df.shape == (3, 3)  # 3 rows and 3 columns in the CSV

# Test create_lambda_function method (mock Lambda client)
@mock_aws
def test_create_lambda_function(s3_setup, secrets_manager_setup):
    # Setup S3Utils
    s3_utils = S3Utils(bucket_name='dataka')

    # Mock role ARN
    role_arn = 'arn:aws:iam::123456789012:role/lambda-role'

    # Mock the Lambda client to avoid actual creation of the function
    with patch.object(s3_utils.lambda_client, 'create_function', return_value={'FunctionName': 'process_csv_lambda'}) as mock_create_lambda:
        # Zip Lambda function and create it
        s3_utils.zip_lambda_function('lambda_function.zip', 'lambda_function.py')  # assuming this file exists for test
        s3_utils.create_lambda_function('process_csv_lambda', role_arn, 'lambda_function.zip')

        # Verify the create function method was called
        mock_create_lambda.assert_called_once()

# Test add_s3_trigger method (mock Lambda permissions and S3 trigger)
@mock_aws
def test_add_s3_trigger(s3_setup, secrets_manager_setup):
    # Setup S3Utils
    s3_utils = S3Utils(bucket_name='dataka')

    # Mock account ID and role ARN
    account_id = '123456789012'
    role_arn = 'arn:aws:iam::123456789012:role/lambda-role'

    # Mock add_permission and put_bucket_notification_configuration methods
    with patch.object(s3_utils.lambda_client, 'add_permission') as mock_add_permission, \
         patch.object(s3_utils.s3_client, 'put_bucket_notification_configuration') as mock_put_notification:
        
        s3_utils.add_s3_trigger('process_csv_lambda', account_id, 'eu-north-1')

        # Verify that the permission and notification methods were called
        mock_add_permission.assert_called_once()
        mock_put_notification.assert_called_once()

