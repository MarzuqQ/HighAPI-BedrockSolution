# Bedrock Request Smoother

A serverless solution for managing and smoothing API request traffic to Amazon Bedrock.

## Architecture Components

### AWS Services Used

- **Lambda Functions**
  - Request Ingestion
  - Prioritization
- **DynamoDB Tables**
  - Requests Table (request tracking)
  - Reporting Table (analytics)
- **SQS Queues**
  - Input Queue
  - Bedrock Results Queue
  - Hold Next Batch Queue (with DLQ)
- **S3**
  - Bedrock Calls Bucket (request storage)
- **CloudWatch**
  - Metrics Dashboard
  - Alarms
- **SNS**
  - Error Notifications

## Deployment Options

You can deploy this solution using either AWS CDK or Terraform.

### Prerequisites

- AWS CLI configured
- Python 3.11
- Node.js 14.x or later (for CDK)
- AWS CDK CLI (for CDK deployment)
- Terraform >= 1.0 (for Terraform deployment)

### Option 1: CDK Deployment

1. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2. Configure the deployment in `cdk.json`:

    ```json
    {
      "app": "python3 app.py",
      "context": {
        "notification_email": "your.email@example.com",
        "average_request_duration": 5,
        "minutes_to_process": 5,
        "max_concurrent_executions": 30,
        "alarm_threshold": 5,
        "alarm_evaluation_periods": 1,
        "dashboard_name": "bedrock-errors"
      }
    }
    ```

3. Deploy the stack:

    ```bash
    cdk deploy
    ```

### Option 2: Terraform Deployment

1. Configure variables in `terraform.tfvars`:

    ```hcl
    aws_region = "us-east-1"
    notification_email = "your.email@example.com"
    ```

2. Initialize and apply Terraform:

    ```bash
    cd br_request_smoother/terraform
    terraform init
    terraform apply
    ```

## Configuration Variables

| Variable | Description | Default |
|----------|-------------|---------|
| aws_region | AWS deployment region | us-east-1 |
| notification_email | Email for error notifications | Required |
| average_request_duration | Average request duration in seconds | 5 |
| minutes_to_process | Processing window in minutes | 5 |
| max_concurrent_executions | Maximum Lambda concurrency | 30 |
| alarm_threshold | Error threshold for alarms | 5 |
| alarm_evaluation_periods | Periods to evaluate for alarms | 1 |
| dashboard_name | CloudWatch dashboard name | bedrock-errors |
| queue_depth_threshold | Max messages in queue | 100 |

## Monitoring

### CloudWatch Dashboard

- Accessible at: `https://console.aws.amazon.com/cloudwatch/home?region={region}#dashboards:name=bedrock-errors`
- Displays Bedrock API error metrics

### Alarms

- Triggers when Bedrock API errors exceed threshold
- Notifications sent via SNS to configured email

## Resource Outputs

After deployment, the following resource identifiers are available:

- Input Queue URL
- Bedrock Results Queue URL
- Hold Next Batch Queue URL
- Requests Table Name
- Reporting Table Name
- Bedrock Calls Bucket Name
- Alarm Topic ARN

## IAM Permissions

The solution creates a Lambda execution role with basic execution permissions. The role includes:

- AWSLambdaBasicExecutionRole
- Custom permissions for S3, DynamoDB, and SQS access

## Error Handling

- Failed requests are tracked in DynamoDB
- Dead Letter Queue configured for failed batch processing
- Error notifications sent via SNS
- CloudWatch alarms monitor error rates

## Cleanup

### CDK Cleanup

```bash
cdk destroy
```

### Terraform Cleanup

```bash
cd br_request_smoother/terraform
terraform destroy
```

Note: S3 bucket must be empty before destruction.
