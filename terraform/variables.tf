variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "notification_email" {
  description = "Email address for alarm notifications"
  type        = string
}

variable "average_request_duration" {
  description = "Average duration of requests in seconds"
  type        = number
  default     = 5
}

variable "minutes_to_process" {
  description = "Minutes to process requests"
  type        = number
  default     = 5
}

variable "max_concurrent_executions" {
  description = "Maximum concurrent Lambda executions"
  type        = number
  default     = 30
}

variable "alarm_threshold" {
  description = "Threshold for Bedrock errors alarm"
  type        = number
  default     = 5
}

variable "alarm_evaluation_periods" {
  description = "Number of periods to evaluate for alarm"
  type        = number
  default     = 1
}

variable "dashboard_name" {
  description = "Name of the CloudWatch dashboard"
  type        = string
  default     = "bedrock-errors"
}

variable "queue_depth_threshold" {
  description = "Maximum number of messages allowed in queue"
  type        = number
  default     = 100
}