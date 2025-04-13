import boto3
import time
from aws_lambda import S3Utils

def poll_sqs_queue(sqs_client, queue_url):
    while True:
        try:
            # Receive messages from the queue
            response = sqs_client.receive_message(
                QueueUrl=queue_url,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,  # Adjust if you want more messages at once
                WaitTimeSeconds=20  # Long poll for 20 seconds
            )

            # If there are messages in the queue
            if 'Messages' in response:
                for message in response['Messages']:
                    # Process the message (this is the S3 event notification)
                    print("Received message:", message['Body'])

                    # Delete the message from the queue after processing
                    sqs_client.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=message['ReceiptHandle']
                    )

        except Exception as e:
            print(f"Error receiving messages from SQS: {e}")
            time.sleep(5)

if __name__ == "__main__":
    # Initialize S3Utils instance and get the queue URL
    bucket_name = 'your-bucket-name'
    s3_utils = S3Utils(bucket_name)
    
    # Ensure the queue is created first and get the URL
    queue_url = s3_utils.create_sqs_queue('my-sqs-queue')  # This creates the queue and returns the URL
    
    if queue_url:
        # Set up SQS client
        sqs_client = boto3.client('sqs', region_name='eu-north-1')
        
        print(f"Starting to poll the SQS queue at {queue_url}...")
        poll_sqs_queue(sqs_client, queue_url)
    else:
        print("Failed to create or retrieve SQS Queue URL.")
