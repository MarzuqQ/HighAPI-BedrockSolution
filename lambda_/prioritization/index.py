import boto3
import json
import random
import logging
from datetime import datetime, timedelta
import os
import hashlib
from botocore.exceptions import ClientError
from decimal import Decimal

# Logger setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
sqs_client = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
service_quotas = boto3.client('service-quotas', region_name=os.environ.get("REGION", 'us-east-1'))

AVERAGE_REQUEST_DURATION_IN_SECONDS = os.environ.get("AVERAGE_REQUEST_DURATION_IN_SECONDS", 3)
QUEUE_DEPTH_THRESHOLD = os.environ.get("QUEUE_DEPTH_THRESHOLD", 100)

# Enhance models dictionary with more metadata
models = {
    'anthropic_claude_3_haiku': {
        'id': 'anthropic.claude-3-haiku-20240307-v1:0',
        'name': 'Anthropic Claude 3 Haiku',
        'rpm_quota_code': "L-2DC80978",
        'tpm_quota_code': "L-8CE99163"
    }
}

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    """
    Custom JSON encoder for handling Decimal types.
    
    Extends the standard JSON encoder to properly serialize Decimal objects
    by converting them to float values.
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def get_service_quotas(region):
    """
    Retrieves AWS Bedrock service quotas for specified models in the given region.
    
    Args:
        region (str): AWS region to query quotas from
        
    Returns:
        dict: Dictionary containing RPM and TPM quotas for each model
              Format: {
                  'model_name': {
                      'RPM': float,
                      'RPM_CODE': str,
                      'TPM': float,
                      'TPM_CODE': str
                  }
              }
              
    Raises:
        ValueError: If region is not specified
        ClientError: If AWS API call fails
        Exception: For unexpected errors
    """
    # Add input validation
    if not region:
        raise ValueError("Region must be specified")
    
    logger.debug(f'Getting service quotas for region: {region}')
    quotas = {}
    try:
        # Get quotas for each model using their specific quota codes
        logger.debug(f"Starting quota retrieval for models: {list(models.keys())}")
        for model_name, model_info in models.items():
            logger.debug(f"Getting quotas for model: {model_name}")
            logger.debug(f"Model info: {json.dumps(model_info)}")
            quotas[model_name] = {}
            
            # Get RPM quota
            logger.debug(f"Getting RPM quota with code: {model_info['rpm_quota_code']}")
            rpm_response = service_quotas.get_service_quota(
                ServiceCode='bedrock',
                QuotaCode=model_info['rpm_quota_code']
            )
            logger.debug(f"RPM quota response: {json.dumps(rpm_response)}")
            quotas[model_name]['RPM'] = rpm_response['Quota']['Value']
            quotas[model_name]['RPM_CODE'] = model_info['rpm_quota_code']
            
            # Get TPM quota  
            logger.debug(f"Getting TPM quota with code: {model_info['tpm_quota_code']}")
            tpm_response = service_quotas.get_service_quota(
                ServiceCode='bedrock',
                QuotaCode=model_info['tpm_quota_code']
            )
            logger.debug(f"TPM quota response: {json.dumps(tpm_response)}")
            quotas[model_name]['TPM'] = tpm_response['Quota']['Value']
            quotas[model_name]['TPM_CODE'] = model_info['tpm_quota_code']

        logger.debug(f"Retrieved quotas: {json.dumps(quotas, cls=DecimalEncoder)}")
        return quotas

    except ClientError as e:
        logger.error(f"AWS ClientError retrieving service quotas: {str(e)}")
        # Return default values instead of empty dict on error
        return {model: {'RPM': info['default_rpm'], 'TPM': info['default_tpm']} 
                for model, info in models.items()}
    except Exception as e:
        logger.error(f"Unexpected error retrieving service quotas: {str(e)}")
        raise

def send_message_to_sqs(sqs_queue_url, request, sent_count):
    """
    Sends a message to SQS queue with deduplication handling.
    
    Args:
        sqs_queue_url (str): URL of the SQS queue
        request (dict): Request data to be sent
        sent_count (int): Current count of successfully sent messages
        
    Returns:
        int: Updated count of successfully sent messages
        
    Raises:
        ClientError: If SQS API call fails
        Exception: For unexpected errors
    """
    try:
        request_id = request['request_id']
        message_body = json.dumps({
            'request_id': request_id,
            'timestamp': datetime.utcnow().isoformat(),
            'data': request
        }, cls=DecimalEncoder)
        
        # deduplication_id = hashlib.md5(request_id.encode()).hexdigest()

        response = sqs_client.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=message_body,
            # MessageGroupId=os.environ.get("DEFAULT_MESSAGE_GROUP_ID"),
            # MessageDeduplicationId=deduplication_id
        )
        if response and 'MessageId' in response:
            # Update DynamoDB record status to QUEUED
            dynamodb_table = dynamodb.Table(os.environ['DYNAMO_TABLE'])
            dynamodb_table.update_item(
                Key={'request_id': request_id},
                UpdateExpression='SET #status = :new_status',
                ExpressionAttributeNames={'#status': 'status'},
                ExpressionAttributeValues={':new_status': 'QUEUED'}
            )
            logger.debug(f"Updated DynamoDB status to QUEUED for request_id: {request_id}")
        logger.debug(f"Sent message to SQS: {response['MessageId']} for request_id: {request_id}")
        return sent_count + 1
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.DuplicateMessageContent':
            logger.info(f"Duplicate message detected for request_id: {request_id}. Skipping.")
            return sent_count
        raise
    except Exception as e:
        logger.error(f"Failed to send message to SQS: {str(e)}")
        raise

def check_conditions(event, context):
    """
    Checks if conditions are met to process the next batch of requests.
    
    Args:
        event (dict): Lambda event object
        context (obj): Lambda context object
        
    Returns:
        tuple: (conditions_met, queue_depth, table_count, items)
               - conditions_met (bool): True if conditions are met
               - queue_depth (int): Current SQS queue depth
               - table_count (int): Count of eligible items
               - items (list): List of eligible items if conditions met, else None
    """
    sqs = boto3.client('sqs')
    table = dynamodb.Table(os.environ['DYNAMO_TABLE'])
    
    try:
        # Check SQS queue depth
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=os.environ['HOLD_NEXT_BATCH_QUEUE_URL'],
            AttributeNames=['ApproximateNumberOfMessages']
        )
        queue_depth = int(queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        
        if queue_depth >= QUEUE_DEPTH_THRESHOLD:
            return False, queue_depth, 0, None
            
        # Scan table for eligible items
        scan_params = {
            'FilterExpression': '#status = :status',
            'ExpressionAttributeNames': {
                '#status': 'status'
            },
            'ExpressionAttributeValues': {
                ':status': 'RECEIVED'
            }
        }
        
        logger.info(f"Scanning table with params: {scan_params}")
        response = table.scan(**scan_params)
        logger.info(f"Scan response: {json.dumps(response, cls=DecimalEncoder)}")
        items = response.get('Items', [])
        logger.info(f"Found {len(items)} items")
        
        # Optional: Print first few items to verify data
        if items:
            logger.info(f"Sample item: {json.dumps(items[0], cls=DecimalEncoder)}")
        
        table_count = len(items)
        
        conditions_met = queue_depth < QUEUE_DEPTH_THRESHOLD and table_count > 0
        return conditions_met, queue_depth, table_count, items if conditions_met else None
        
    except ClientError as e:
        logger.error(f"Error checking conditions: {str(e)}")
        return False, 0, 0, None

def handler(event, context):
    """
    Main Lambda handler for request prioritization and queue management.
    
    Checks queue depth and processes pending requests based on service quotas
    and prioritization rules. Sends prioritized requests to SQS queue for
    processing.
    
    Args:
        event (dict): Lambda event object
        context (obj): Lambda context object
        
    Returns:
        dict: Response object with status code and message
              Format: {
                  'statusCode': int,
                  'body': str or dict
              }
        
    Raises:
        ValueError: If required environment variables are missing
        Exception: For unexpected errors
    """
    # Check conditions before processing
    conditions_met, queue_depth, table_count, items = check_conditions(event, context)
    if not conditions_met:
        logger.info(f"Conditions not met, skipping processing queue_depth: {queue_depth} table_count: {table_count}")
        return {
            'statusCode': 200,
            'body': f'Conditions not met for processing queue_depth: {queue_depth} table_count: {table_count}'
        }
    
    logger.debug(f"Received event: {json.dumps(event)}")
    sqs_queue_url = os.environ.get("HOLD_NEXT_BATCH_QUEUE_URL")
    table_name = os.environ.get("DYNAMO_TABLE")
    sent_count = 0
    
    try:
        if not sqs_queue_url or not table_name:
            raise ValueError("Missing required environment variables: SQS_QUEUE_URL or DYNAMO_TABLE")

        logger.info(f"Found {len(items)} requests for prioritization")

        # Apply prioritization logic - oldest first
        prioritized_requests = sorted(items, key=lambda x: x.get('created_at', ''))
        logger.debug(f"Prioritized requests: {json.dumps(prioritized_requests, cls=DecimalEncoder)}")

        # Check Bedrock service quotas
        quotas = get_service_quotas(os.environ.get("REGION"))
        logger.debug(f"Retrieved quotas: {json.dumps(quotas, cls=DecimalEncoder)}")
        
        concurrent_requests = int(quotas.get('anthropic_claude_3_haiku', {}).get('RPM')/int(os.environ.get("AVERAGE_REQUEST_DURATION_IN_SECONDS", AVERAGE_REQUEST_DURATION_IN_SECONDS)))
        max_requests = min(concurrent_requests*int(os.environ.get("MINUTES_TO_PROCESS", 5)), len(prioritized_requests))
        logger.info(f"Will attempt to send {max_requests} requests to SQS")
        
        for request in prioritized_requests[:max_requests]:
            sent_count = send_message_to_sqs(sqs_queue_url, request, sent_count)

        logger.info(f"Successfully sent {sent_count} messages to SQS")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}", exc_info=True)
        raise

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Prioritization and quota check complete" if sent_count > 0 else "No messages sent",
            "messages_sent": sent_count
        })
    }
