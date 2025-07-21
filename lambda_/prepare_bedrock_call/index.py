import json
import boto3
import os
from botocore.exceptions import ClientError
from datetime import datetime
import logging
from typing import Dict, List, Union, Tuple
from functools import wraps
import time

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
bedrock = boto3.client('bedrock-runtime')


def timeit(func):
    """
    Decorator that measures and logs the execution time of functions.

    Args:
        func: The function to be timed

    Returns:
        wrapper: Decorated function that includes timing functionality
    """
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        # Store the execution time as an attribute of the wrapper function
        timeit_wrapper.execution_time = total_time
        print(f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    timeit_wrapper.execution_time = 0  # Initialize the attribute
    return timeit_wrapper


def update_dynamodb_error(table, request_id, error_message):
    table.update_item(
        Key={'request_id': request_id},
        UpdateExpression="SET request_status = :status, updated_at = :time, error_message = :error",
        ExpressionAttributeValues={
            ':status': 'FAILED',
            ':time': datetime.utcnow().isoformat(),
            ':error': error_message
        }
    )
    logger.info(f"Updated DynamoDB for request {request_id}: status set to FAILED")

def get_request_data(s3_client, bucket: str, request_id: str) -> Dict:
    """
    Retrieve and validate request data from S3.

    Args:
        s3_client: Boto3 S3 client instance
        bucket (str): Name of the S3 bucket containing the request data
        request_id (str): Unique identifier for the request

    Returns:
        Dict: Validated JSON data containing the request parameters

    Raises:
        ValueError: If JSON is invalid or S3 retrieval fails
        json.JSONDecodeError: If the S3 object contains invalid JSON
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=f'{request_id}.json')
        request_data_str = response['Body'].read().decode('utf-8')
        request_data = json.loads(request_data_str)
        logger.info(f"Request data for {request_id}: {json.dumps(request_data)}")
        return request_data
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON format for request {request_id}")
    except Exception as e:
        raise ValueError(f"Failed to retrieve request data: {str(e)}")

def update_request_status(table, request_id: str, status: str) -> None:
    """
    Update request status and timestamp in DynamoDB.

    Args:
        table: DynamoDB table instance
        request_id (str): Unique identifier for the request
        status (str): New status to set ('IN_PROGRESS', 'COMPLETED', 'FAILED')

    Raises:
        Exception: If DynamoDB update operation fails
    """
    table.update_item(
        Key={'request_id': request_id},
        UpdateExpression="SET #status = :status, updated_at = :time",
        ExpressionAttributeNames={'#status': 'status'},
        ExpressionAttributeValues={
            ':status': status,
            ':time': datetime.utcnow().isoformat()
        }
    )
    logger.info(f"Updated DynamoDB for request {request_id}: status set to {status}")

def prepare_bedrock_params(request_data: Dict) -> Dict:
    """
    Extract and validate parameters for Bedrock API call.

    Args:
        request_data (Dict): Request data containing:
            - messages (List): List of message objects for the conversation
            - model_id (str): Bedrock model identifier
            - system (List, optional): System prompt messages

    Returns:
        Dict: Formatted parameters for Bedrock API call containing:
            - modelId (str): The Bedrock model identifier
            - messages (List): The conversation messages
            - system (List, optional): System prompt if provided

    Raises:
        ValueError: If required fields (messages or model_id) are missing
    """
    messages = request_data.get('messages', [])
    model_id = request_data.get('model_id')
    system = request_data.get('system', [])

    if not (messages and model_id):
        raise ValueError("Missing required fields: 'messages' or 'model_id'")

    params = {
        "modelId": model_id,
        "messages": messages,
    }
    
    if system:
        params["system"] = system
        
    return params

def process_bedrock_response(response: Dict) -> Union[Dict, str]:
    """
    Process and validate Bedrock API response.

    Args:
        response (Dict): Raw response from Bedrock API containing:
            - output (Dict): Output container
            - message (Dict): Message container with content

    Returns:
        Union[Dict, str]: Either:
            - Dict: Parsed JSON response if completion is valid JSON
            - str: Raw completion text if not valid JSON

    Raises:
        ValueError: If response structure is invalid or missing required fields
    """
    if 'output' not in response or 'message' not in response['output']:
        raise ValueError("Invalid response structure from Bedrock API")

    message_content = response['output']['message']
    
    if 'content' not in message_content or not message_content['content']:
        raise ValueError("No content in message response")

    completion = message_content['content'][0].get('text', "No content returned")

    try:
        return json.loads('{' + completion), response['metrics'], response['usage'] # the '{' is needed to keep claude from being chatty
    except json.JSONDecodeError:
        logger.warning("Failed to parse completion as JSON")
        return completion

@timeit
def process_record(record: Dict, s3, dynamodb_table, bedrock_client, bucket: str) -> Tuple[str, List, Union[Dict, str]]:
    """
    Process a single SQS record through the entire workflow.

    Args:
        record (Dict): SQS record containing the message body
        s3: Boto3 S3 client instance
        dynamodb_table: DynamoDB table instance
        bedrock_client: Bedrock client instance
        bucket (str): Name of the S3 bucket

    Returns:
        Tuple[str, List, Union[Dict, str]]: Tuple containing:
            - request_id (str): Unique identifier for the request
            - messages (List): Original messages from the request
            - result (Union[Dict, str]): Processed Bedrock response

    Raises:
        Exception: If any step in the processing pipeline fails
    """
    message = json.loads(record['body'])
    request_id = message['request_id']
    
    try:
        request_data = get_request_data(s3, bucket, request_id)
        update_request_status(dynamodb_table, request_id, 'BEDROCK_CALL_IN_PROGRESS')
        
        params = prepare_bedrock_params(request_data)
        bedrock_response = bedrock_client.converse(**params)
        logger.debug(f"Raw Bedrock response: {bedrock_response}")
        
        result = process_bedrock_response(bedrock_response)

        
        return request_id, result , bedrock_response['metrics'], bedrock_response['usage']
        
    except Exception as e:
        logger.error(f"Error processing request {request_id}: {str(e)}")
        update_request_status(dynamodb_table, request_id, 'FAILED')
        raise

def handler(event: Dict, context) -> Dict:
    """
    Main Lambda handler for processing Bedrock requests.

    Args:
        event (Dict): Lambda event containing:
            - Records (List): List of SQS records to process
        context: Lambda context object (unused)

    Returns:
        Dict: Response object containing:
            - statusCode (int): HTTP status code (200 for success)
            - body (str): JSON-encoded success message

    Note:
        This function:
        1. Processes each record in the SQS batch
        2. Updates DynamoDB status throughout processing
        3. Calls Bedrock API for each request
        4. Sends results to a results SQS queue
        5. Handles errors and updates status accordingly
    """
    # Environment variables
    S3_BUCKET = os.environ['S3_BUCKET_NAME']
    DYNAMODB_TABLE = os.environ['DYNAMODB_TABLE_NAME']
    RESULTS_QUEUE_URL = os.environ['RESULTS_QUEUE_URL']

    logger.debug(f"Event: {event}")
    for record in event['Records']:
        request_id = None
        try:
            # Extract request_id before processing
            message = json.loads(record['body'])
            request_id = message['request_id']
            
            # Process the record
            request_id, result, metrics, usage = process_record(record, s3, dynamodb.Table(DYNAMODB_TABLE), bedrock, S3_BUCKET)
            logger.info(f"process_record took {process_record.execution_time:.2f} seconds for request {request_id}")
            
            # Prepare and send SQS message
            sqs_message = {
                'request_id': request_id,
                'original_request': result,
                'result': result,
                'metrics': metrics,
                'usage': usage
            }

            sqs_response = sqs.send_message(
                QueueUrl=RESULTS_QUEUE_URL,
                MessageBody=json.dumps(sqs_message) 
            )
            logger.info(f"Message sent to SQS. MessageId: {sqs_response['MessageId']}")

            # Update DynamoDB to mark success
            update_request_status(dynamodb.Table(DYNAMODB_TABLE), request_id, 'COMPLETED')
            
        except Exception as e:
            error_msg = f"Unexpected error processing request {request_id or 'UNKNOWN'}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if request_id:  # Only update DynamoDB if we have a request_id
                update_dynamodb_error(dynamodb.Table(DYNAMODB_TABLE), request_id, str(e))
            raise
        
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processing complete for {request_id} with metrics {metrics} and usage {usage}')
    }
