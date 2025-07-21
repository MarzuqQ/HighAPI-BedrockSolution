output "input_queue_url" {
  value = aws_sqs_queue.input.url
}

output "bedrock_results_queue_url" {
  value = aws_sqs_queue.bedrock_results.url
}

output "hold_next_batch_queue_url" {
  value = aws_sqs_queue.hold_next_batch.url
}

output "requests_table_name" {
  value = aws_dynamodb_table.requests.name
}

output "reporting_table_name" {
  value = aws_dynamodb_table.reporting.name
}

output "bedrock_calls_bucket_name" {
  value = aws_s3_bucket.bedrock_calls.id
}

output "alarm_topic_arn" {
  value = aws_sns_topic.alarm.arn
}