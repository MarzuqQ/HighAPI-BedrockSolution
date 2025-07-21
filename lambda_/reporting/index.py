import os
import json
import boto3
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
reporting_table = dynamodb.Table(os.environ['REPORTING_TABLE'])

def process_dynamodb_image(image: Dict) -> Dict[str, str]:
    """
    Extract and validate fields from a DynamoDB Stream image.

    Args:
        image (Dict): DynamoDB Stream image containing attribute values in DynamoDB format
            Example: {'request_id': {'S': '123'}, 'request_status': {'S': 'COMPLETED'}}

    Returns:
        Dict[str, str]: Extracted fields in standard format
            Example: {'request_id': '123', 'request_status': 'COMPLETED'}

    Raises:
        KeyError: If required fields are missing from the image
        ValueError: If field values are in unexpected format
    """
    try:
        return {
            'request_id': image['request_id']['S'],
            'status': image.get('status', {}).get('S', 'UNKNOWN'),
            'created_at': image.get('created_at', {}).get('S', ''),
            'updated_at': image.get('updated_at', {}).get('S', '')
        }
    except KeyError as e:
        logger.error(f"Missing required field in DynamoDB image: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing DynamoDB image: {str(e)}")
        raise

def create_reporting_item(request_data: Dict[str, str], event_type: str) -> Dict[str, str]:
    """
    Create a reporting item from request data and event type.

    Args:
        request_data (Dict[str, str]): Processed request data containing:
            - request_id (str): Unique identifier for the request
            - request_status (str): Current status of the request
            - created_at (str): ISO format timestamp of request creation
            - updated_at (str): ISO format timestamp of last update
        event_type (str): DynamoDB Stream event type (INSERT/MODIFY/REMOVE)

    Returns:
        Dict[str, str]: Formatted item for reporting table containing:
            - report_id (str): Unique identifier for the report entry
            - request_id (str): Original request identifier
            - status (str): Request status at time of event
            - timestamp (str): ISO format timestamp of report creation
            - event_type (str): Type of DynamoDB Stream event
            - created_at (str): Original request creation time
            - updated_at (str): Last request update time

    Raises:
        ValueError: If required fields are missing or in invalid format
    """
    timestamp = datetime.utcnow().isoformat()
    return {
        'report_id': f"{request_data['request_id']}_{timestamp}",
        'request_id': request_data['request_id'],
        'status': request_data['status'],
        'timestamp': timestamp,
        'event_type': event_type,
        'created_at': request_data['created_at'],
        'updated_at': request_data['updated_at']
    }

def process_record(record: Dict) -> None:
    """
    Process a single DynamoDB Stream record and write to reporting table.

    Args:
        record (Dict): DynamoDB Stream record containing:
            - eventName (str): Type of change (INSERT/MODIFY/REMOVE)
            - dynamodb (Dict): Change data including NewImage/OldImage
            - eventID (str): Unique identifier for the stream record
            - eventVersion (str): Version number of the stream record

    Raises:
        KeyError: If required fields are missing from the stream record
        ValueError: If record data is invalid
        boto3.exceptions.Boto3Error: If DynamoDB operation fails
    """
    try:
        # Skip if record is a deletion
        if 'NewImage' not in record['dynamodb']:
            logger.info(f"Skipping deletion event for record: {record['eventID']}")
            return

        # Process the DynamoDB image
        request_data = process_dynamodb_image(record['dynamodb']['NewImage'])
        
        # Create and write reporting item
        reporting_item = create_reporting_item(request_data, record['eventName'])
        reporting_table.put_item(Item=reporting_item)
        
        logger.info(f"Successfully wrote report for request {request_data['request_id']}")

    except Exception as e:
        logger.error(f"Error processing record {record.get('eventID', 'unknown')}: {str(e)}")
        raise

def handler(event: Dict, context) -> Dict:
    """
    Main Lambda handler for processing DynamoDB Stream events.

    Args:
        event (Dict): DynamoDB Stream event containing:
            - Records (List[Dict]): List of DynamoDB Stream records
                Each record contains change data for a single item
            - eventSource (str): Source of the event (aws:dynamodb)
            - awsRegion (str): AWS region where the event occurred
        context: Lambda context object (unused)

    Returns:
        Dict: Response object containing:
            - statusCode (int): HTTP status code (200 for success)
            - body (str): JSON-encoded success message with count of processed records

    Raises:
        Exception: If processing fails for any record in the batch

    Note:
        This function:
        1. Processes each record from the DynamoDB Stream
        2. Creates corresponding entries in the reporting table
        3. Handles errors and provides detailed logging
        4. Maintains an audit trail of all request status changes
    """
    try:
        processed_count = 0
        for record in event['Records']:
            process_record(record)
            processed_count += 1

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully processed all records',
                'processed_count': processed_count
            })
        }
    except Exception as e:
        logger.error(f"Error in handler: {str(e)}")
        raise