terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket
resource "aws_s3_bucket" "bedrock_calls" {
  bucket_prefix = "bedrock-calls-"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "bedrock_calls" {
  bucket = aws_s3_bucket.bedrock_calls.id
  versioning_configuration {
    status = "Enabled"
  }
}

# DynamoDB Tables
resource "aws_dynamodb_table" "requests" {
  name           = "requests-table"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "request_id"
  stream_enabled = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "request_id"
    type = "S"
  }
}

resource "aws_dynamodb_table" "reporting" {
  name         = "reporting-table"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "report_id"

  attribute {
    name = "report_id"
    type = "S"
  }
}

# SQS Queues
resource "aws_sqs_queue" "input" {
  name                      = "input-queue"
  visibility_timeout_seconds = 300
}

resource "aws_sqs_queue" "bedrock_results" {
  name                      = "bedrock-results-queue"
  visibility_timeout_seconds = 300
}

resource "aws_sqs_queue" "hold_next_batch_dlq" {
  name                      = "hold-next-batch-dlq"
  visibility_timeout_seconds = 300
}

resource "aws_sqs_queue" "hold_next_batch" {
  name                      = "hold-next-batch-queue"
  visibility_timeout_seconds = 300
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.hold_next_batch_dlq.arn
    maxReceiveCount     = 3
  })
}