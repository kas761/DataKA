import boto3
import zipfile
import os
import logging
import json
import uuid
import pandas as pd
from io import StringIO

region = 'eu-north-1'
bucket_name = 'dataka'
files = ['winequality-red.csv', 'winequality-white.csv']
lambda_name = 'process_csv_lambda'
secret_name = 'secret'

# Initialize the clients
s3_client = boto3.client('s3', region_name=region)
lambda_client = boto3.client('lambda', region_name=region)
secrets_manager_client = boto3.client('secretsmanager', region_name=region)

# ARN for the AWSSDKPandas-Python38 Lambda layer
LAYER_ARN = f'arn:aws:lambda:eu-north-1:336392948345:layer:AWSSDKPandas-Python38:29'  # Update the ARN if necessary

class S3Utils:
    def __init__(self, bucket_name, region=region):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=region)
        self.lambda_client = boto3.client('lambda', region_name=region)
        self.secrets_manager_client = boto3.client('secretsmanager', region_name=region)
        self.create_s3_bucket()

    def list_s3_buckets(self):
        response = self.s3_client.list_buckets()
        print("S3 Buckets:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")

    def check_aws_connection(self):
        response = self.s3_client.list_buckets()
        if response['Buckets']:
            print("Connection successful. Buckets:")
            for bucket in response['Buckets']:
                print(bucket['Name'])
        else:
            print("No buckets found!")

    def get_secret(self, secret_name):
        try:
            response = self.secrets_manager_client.get_secret_value(SecretId=secret_name)
            # Parse the secret value from JSON string
            secret = json.loads(response['SecretString'])
            account_id = secret['AWS_ACCOUNT_ID']
            role_arn = secret['LAMBDA_ROLE_ARN']
            print(f"AWS Account ID: {account_id}, Lambda Role ARN: {role_arn}")
            return account_id, role_arn
        except Exception as e:
            print(f"Error retrieving secret: {e}")
            return None, None

    def create_s3_bucket(self):
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f"Bucket {self.bucket_name} already exists!")
        except:
            try:
                response = self.s3_client.create_bucket(
                    Bucket=self.bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
                print(f"Bucket {self.bucket_name} created successfully!")
                return response
            except Exception as e:
                print(f"Error creating bucket {self.bucket_name}: {e}")
                raise

    def lambda_handler(self, event, context):
        logging.info(f"Event: {json.dumps(event)}")

        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']

        try:
            file_obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            file_content = file_obj['Body'].read().decode('utf-8').splitlines()
            
            df = pd.read_csv(StringIO("\n".join(file_content)))
            print(df.head())

            return {
                'statusCode': 200,
                'body': json.dumps(f"Successfully processed {file_key}")
            }
        except Exception as e:
            logging.error(f"Error processing file {file_key}: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error processing file {file_key}")
            }

    def create_lambda_function(self, function_name, role_arn, zip_file):
        try:
            with open(zip_file, 'rb') as f:
                zipped_code = f.read()

            # Create the Lambda function with the additional layer ARN
            response = self.lambda_client.create_function(
                FunctionName=function_name,
                Runtime='python3.8',
                Role=role_arn,
                Handler='lambda_function.lambda_handler',
                Code=dict(ZipFile=zipped_code),
                Timeout=300,
                Layers=[LAYER_ARN],  # Attach the AWSSDKPandas-Python38 layer
            )
            print(f"Lambda function {function_name} created successfully.")
        except Exception as e:
            print(f"Error creating Lambda function: {e}")

    def zip_lambda_function(self, zip_file, source_file):
        with zipfile.ZipFile(zip_file, 'w') as z:
            z.write(source_file, os.path.basename(source_file))

    def add_s3_trigger(self, lambda_function_name, aws_account_id, region):
        lambda_arn = f'arn:aws:lambda:{region}:{aws_account_id}:function:{lambda_function_name}'
        notification = {
            'LambdaFunctionConfigurations': [
                {
                'LambdaFunctionArn': lambda_arn,
                'Events': ['s3:ObjectCreated:*'],
                'Filter': {
                    'Key': {
                        'FilterRules': [
                            {
                                'Name': 'suffix',
                                'Value': '.csv'  # Only trigger for .csv files
                            },
                            {
                                'Name': 'prefix',
                                'Value': 'winequality-'  # Only trigger for files with 'winequality-' in the name
                            }
                        ]
                    }
                }
            }
        ]
    }

        try:
            # Add permission for S3 to invoke Lambda
            self.lambda_client.add_permission(
                FunctionName=lambda_function_name,
                StatementId=f"unique-statement-id-{uuid.uuid4()}",
                Action="lambda:InvokeFunction",
                Principal="s3.amazonaws.com",
                SourceArn=f"arn:aws:s3:::{self.bucket_name}"
            )
            
            # Set up the bucket notification
            self.s3_client.put_bucket_notification_configuration(
                Bucket=self.bucket_name,
                NotificationConfiguration=notification
            )
            print(f"Trigger added for {lambda_function_name} on bucket {self.bucket_name}")
        except Exception as e:
            print(f"Error adding trigger: {e}")

    def upload_files_to_s3(self, files):
        for file in files:
            file_key = os.path.basename(file)
            try:
                self.s3_client.upload_file(file, self.bucket_name, file_key)
                print(f"File uploaded successfully to {self.bucket_name}/{file_key}")
            except Exception as e:
                print(f"Error uploading file {file}: {e}")


if __name__ == '__main__':
    s3_utils = S3Utils(bucket_name)

    # List S3 buckets
    s3_utils.list_s3_buckets()

    # Check AWS connection
    s3_utils.check_aws_connection()

    # Get secret (account ID and role ARN)
    account_id, role_arn = s3_utils.get_secret(secret_name)

    if account_id and role_arn:
        # Zip the Lambda function
        s3_utils.zip_lambda_function('lambda_function.zip', 'lambda_function.py')

        # Create Lambda function
        s3_utils.create_lambda_function(lambda_name, role_arn, 'lambda_function.zip')

        # Add the trigger to the S3 bucket
        s3_utils.add_s3_trigger(lambda_name, account_id, region)

        # Upload files to the bucket
        s3_utils.upload_files_to_s3(files)
