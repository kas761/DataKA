from fastapi import FastAPI, Query, HTTPException, Header, Depends
import boto3
import json
import os
import logging

# uvicorn fast_api:app --reload

# Initialize boto3 client
REGION = 'eu-north-1'
BUCKET_NAME = 'dataka'
S3_CLIENT = boto3.client('s3', region_name=REGION)
API_KEY = os.getenv('API_KEY')

class DataProcessor:
    def __init__(self, region: str, bucket_name: str):
        self.s3_client = boto3.client('s3', region_name=region)
        self.bucket_name = bucket_name
        logging.basicConfig(level=logging.DEBUG)
    
    def process_json_data(self, file_key: str):
        try:
            # Fetch the file from S3
            print(f"Fetching file from bucket: {self.bucket_name}, key: {file_key}")
            file_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            file_content = file_obj['Body'].read().decode('utf-8')
            
            # Parse JSON data
            data = json.loads(file_content)
            
            return data
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

        
    def get_file_key(self, quality: str):
        # Select the correct file based on the quality parameter
        cases = {
            'high': 'high_quality_average.json',
            'low': 'low_quality_average.json'
        }
        
        if quality not in cases:
            raise HTTPException(status_code=400, detail="Invalid quality parameter. Allowed values: high, low")
        
        return cases[quality]


# FastAPI instance
app = FastAPI()

# Instantiate the DataProcessor class
data_processor = DataProcessor(region=REGION, bucket_name=BUCKET_NAME)

@app.get("/process_data")
async def process_data_endpoint(quality: str = Query(..., alias="qualityquery"), api_key: str = Header(API_KEY)):
    # Check if API key is provided
    if api_key != os.getenv('API_KEY'):
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")
    
    # Ensure the quality is either 'high' or 'low'
    if quality not in ['high', 'low']:
        raise HTTPException(status_code=400, detail="Invalid quality query parameter. Allowed values: high, low")
    
    # Get the correct file key based on the quality
    file_key = data_processor.get_file_key(quality)
    
    # Process the selected JSON file
    result = data_processor.process_json_data(file_key)
    
    return result
