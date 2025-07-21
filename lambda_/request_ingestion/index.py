import boto3
import json
import uuid
import logging
import os
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get environment variables
S3_BUCKET = os.environ['S3_BUCKET']
REQUESTS_TABLE = os.environ['REQUESTS_TABLE']

def process_message(message):
    """
    Processes a single message by storing it in S3 and recording metadata in DynamoDB.
    
    Creates a unique identifier for the request and stores the raw request data
    in S3 while maintaining request metadata in DynamoDB for tracking purposes.
    
    Args:
        message (dict): The message payload to process
    """
    # Generate unique ID using UUID
    request_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()
    
    try:
        # Store raw data in S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f'{request_id}.json',
            Body=json.dumps(message)
        )
        logger.info("Successfully stored request data in S3 with ID: %s", request_id)
        
        # Store metadata in DynamoDB
        table = dynamodb.Table(REQUESTS_TABLE)
        table.put_item(
            Item={
                'request_id': request_id,
                'status': 'RECEIVED',
                'created_at': timestamp,
                ###### add any other attributes here
            }
        )
        logger.info("Successfully stored request metadata in DynamoDB with ID: %s", request_id)
        
    except Exception as e:
        logger.error("Error processing request %s: %s", request_id, str(e))
        raise

def handler(event, context):
    """
    Main entry point for the Lambda function that processes incoming requests.
    
    Handles both direct invocations and SQS event sources. For SQS events, 
    processes each record in the batch. For direct invocations, processes 
    the event directly.
    
    Args:
        event (dict): The event data from the Lambda trigger
        context (LambdaContext): Runtime information provided by AWS Lambda
    
    Returns:
        dict: Response object with status code
    """
    logger.debug("Received event: %s", json.dumps(event))

    # Check if the event is from SQS
    if 'Records' in event:
        for record in event['Records']:
            try:
                message = json.loads(record['body'])
            except KeyError:
                logger.error("Unexpected event structure. 'body' not found in record")
                continue
            except json.JSONDecodeError:
                logger.error("Failed to parse message body as JSON")
                continue

            # Process the message
            process_message(message)
    else:
        # For direct invocation or other event sources
        process_message(event)

    return {'statusCode': 200,
            'body': f'{event}'}
