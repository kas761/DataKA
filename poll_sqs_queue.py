import boto3
import json
import time

def poll_sqs_queue(queue_url, region='eu-north-1'):

    sqs = boto3.client('sqs', region_name=region)
    
    print(f"Starting to poll SQS queue: {queue_url}")
    print("Press Ctrl+C to stop polling")
    print("-" * 50)
    
    try:
        while True:
            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=10
            )
            
            if 'Messages' in response:
                for message in response['Messages']:
                    receipt_handle = message['ReceiptHandle']
                    
                    try:
                        body = json.loads(message['Body'])
                        if 'Message' in body:
                            s3_event = json.loads(body['Message'])
                            if 'Records' in s3_event:
                                for record in s3_event['Records']:
                                    if 's3' in record:
                                        bucket = record['s3']['bucket']['name']
                                        file_name = record['s3']['object']['key']
                                        print(f"New file uploaded: {file_name}")
                                        print(f"Bucket: {bucket}")
                                        print(f"Event type: {record['eventName']}")
                                        
                    except:
                        print("Could not parse message")

                    sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )
            else:
                print(".", end="", flush=True)
                time.sleep(2)
            
    except KeyboardInterrupt:
        print("\nStopping SQS polling")

if __name__ == "__main__":

    queue_url = input("Enter your SQS Queue URL: ")
    poll_sqs_queue(queue_url)