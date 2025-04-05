from fastapi import FastAPI, Query, HTTPException, Header
import boto3
import json
import os

#uvicorn fast_api:app --reload

# Initialize boto3 client
region = 'eu-north-1'
bucket_name = 'dataka'
s3_client = boto3.client('s3', region_name=region)

app = FastAPI()

# Function to check the API key
def check_api_key(api_key: str):
    API_KEY = os.getenv('API_KEY')
    if api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Forbidden: Invalid API Key")

# Function to process JSON data
def process_json_data(file_key: str):
    try:
        # Fetch the file from S3
        file_obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = file_obj['Body'].read().decode('utf-8')
        
        # Parse JSON data
        data = json.loads(file_content)
        
        return data 
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file {file_key}: {e}")

@app.get("/process_data")
async def process_data_endpoint(quality: str = Query(..., alias="qualityquery"), x_api_key: str = Header(...)):
    # Ensure the quality is either 'high' or 'low'
    if quality not in ['high', 'low']:
        raise HTTPException(status_code=400, detail="Invalid quality query parameter. Allowed values: high, low")
    
    # Check the API key
    check_api_key(x_api_key)
    
    # Select the correct file based on the quality parameter
    if quality == 'high':
        file_key = 'high_quality_average.json'
    elif quality == 'low':
        file_key = 'low_quality_average.json'
    
    # Process the selected JSON file
    result = process_json_data(file_key)
    
    return result
