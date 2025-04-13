from fastapi import FastAPI, Query, HTTPException, Header, Depends
import boto3
import json
import os

# uvicorn fast_api:app --reload

REGION = 'eu-north-1'
BUCKET_NAME = 'dataka'
S3_CLIENT = boto3.client('s3', region_name=REGION)
API_KEY = os.getenv('API_KEY')


app = FastAPI()

class DataProcessor:
    def __init__(self, s3_client=S3_CLIENT, region=REGION, bucket_name=BUCKET_NAME):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.region = region

    def process_json_data(self, file_key: str):
        try:
            print(f"Fetching file from bucket: {self.bucket_name}, key: {file_key}")
            file_obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=file_key)
            file_content = file_obj['Body'].read().decode('utf-8')
            return json.loads(file_content)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    def get_file_key(self, quality: str):
        quality_map = {
            'high': 'high_quality_average.json',
            'low': 'low_quality_average.json',
        }
        if quality not in quality_map:
            raise HTTPException(status_code=400, detail="Invalid quality parameter. Allowed values: high, low")
        return quality_map[quality]

def get_data_processor() -> DataProcessor:
    return DataProcessor()

@app.get("/process_data")
async def process_data_endpoint(
    quality: str = Query(..., alias="qualityquery"),
    api_key: str = Header(None),
    processor: DataProcessor = Depends(get_data_processor)
):
    if api_key != os.getenv('API_KEY'):
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")

    file_key = processor.get_file_key(quality)
    result = processor.process_json_data(file_key)
    return result
