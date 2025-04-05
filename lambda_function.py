import json
import pandas as pd
import boto3
from io import StringIO

# Initialize the S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Log the event for debugging purposes
    print(f"Received event: {json.dumps(event)}")

    # Extract bucket name and object keys from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    red_wine_key = 'winequality-red.csv'
    white_wine_key = 'winequality-white.csv'
    
    try:
        # Fetch the red wine and white wine CSV files from S3
        red_wine_obj = s3_client.get_object(Bucket=bucket_name, Key=red_wine_key)
        white_wine_obj = s3_client.get_object(Bucket=bucket_name, Key=white_wine_key)
        
        # Read the CSV contents into DataFrames
        red_wine_content = red_wine_obj['Body'].read().decode('utf-8')
        white_wine_content = white_wine_obj['Body'].read().decode('utf-8')

        red_wine = pd.read_csv(StringIO(red_wine_content), delimiter=';')
        white_wine = pd.read_csv(StringIO(white_wine_content), delimiter=';')

        # Add a new column for wine type
        red_wine["wine_type"] = 'red'
        white_wine["wine_type"] = 'white'

        # Concatenate the two dataframes
        wine = pd.concat([red_wine, white_wine], ignore_index=True)

        # Separate high and low-quality wines
        high_quality_wine = wine[wine["quality"] >= 7]
        low_quality_wine = wine[wine['quality'] <= 4]

        # Calculate average quality for both high and low quality wines
        high_average_quality = round(high_quality_wine['quality'].mean(), 2)
        low_average_quality = round(low_quality_wine['quality'].mean(), 2)

        # Prepare the high and low average quality data as dictionaries
        high_quality_avg_data = {'high_average_quality': high_average_quality}
        low_quality_avg_data = {'low_average_quality': low_average_quality}

        # Convert the dictionaries to JSON strings
        high_quality_avg_json = json.dumps(high_quality_avg_data)
        low_quality_avg_json = json.dumps(low_quality_avg_data)
        
        # Define the S3 keys for saving the high and low quality average data
        high_quality_avg_key = 'high_quality_average.json'
        low_quality_avg_key = 'low_quality_average.json'

        # Upload the high quality average data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=high_quality_avg_key,
            Body=high_quality_avg_json,
            ContentType='application/json'
        )

        # Upload the low quality average data to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=low_quality_avg_key,
            Body=low_quality_avg_json,
            ContentType='application/json'
        )

        # Return the result in the Lambda response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'high_average_quality': high_average_quality,
                'low_average_quality': low_average_quality
            })
        }
    
    except Exception as e:
        # Log any errors
        print(f"Error processing files: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"Error processing files: {str(e)}"
            })
        }
