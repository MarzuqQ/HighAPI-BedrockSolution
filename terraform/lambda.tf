# Lambda Functions
resource "aws_lambda_function" "request_ingestion" {
  filename         = "lambda_/lambda.zip"
  function_name    = "request-ingestion"
  role            = aws_iam_role.lambda_role.arn
  handler         = "request_ingestion.index.handler"
  runtime         = "python3.11"
  timeout         = 30
  memory_size     = 128
  architectures   = ["arm64"]

  environment {
    variables = {
      REQUESTS_TABLE = aws_dynamodb_table.requests.name
      S3_BUCKET     = aws_s3_bucket.bedrock_calls.id
    }
  }
}

resource "aws_lambda_function" "prioritization" {
  filename         = "lambda_/lambda.zip"
  function_name    = "prioritization"
  role            = aws_iam_role.lambda_role.arn
  handler         = "prioritization.index.handler"
  runtime         = "python3.11"
  timeout         = 30
  memory_size     = 128
  architectures   = ["arm64"]

  environment {
    variables = {
      DYNAMO_TABLE = aws_dynamodb_table.requests.name
      REGION = var.aws_region
      AVERAGE_REQUEST_DURATION_IN_SECONDS = var.average_request_duration
      MINUTES_TO_PROCESS = var.minutes_to_process
      DEFAULT_MESSAGE_GROUP_ID = "default"
      HOLD_NEXT_BATCH_QUEUE_URL = aws_sqs_queue.hold_next_batch.url
      QUEUE_DEPTH_THRESHOLD = var.queue_depth_threshold
    }
  }
}

# Similar blocks for prepare_bedrock_call and reporting Lambda functions